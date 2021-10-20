
# Packages required for the view/GUI
import tkinter as tk
import tkinter.filedialog
from tkinter import ttk # New-style widgets
import os, shutil
import threading

# Packages required for the model/data-wrangling
import pandas as pd # type: ignore
import datetime as dt
import platform

# Setup for downloading Eikon data
try:
	import configparser as cp

	cfg = cp.ConfigParser()
	cfg.read('eikon.cfg')  # adjust for different file location
	print(f"Eikon app id is {cfg['eikon']['app_id']}")

	import eikon as ek # type: ignore

	ek.set_app_key(cfg['eikon']['app_id'])
	print("Connected to Eikon Data API!")
	EIKON_CONNECTION = True

	import logging
	eikonLogger = logging.getLogger('pyeikon')
	eikonLogger.setLevel(logging.FATAL)

except Exception as e:
	print(f"Exception when trying to connect to Eikon Data API: {e}")
	EIKON_CONNECTION = False

from typing import Callable, Dict, List, Tuple

# Valid intervals for Eikon are:
# 'tick', 'minute', 'hour', 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'.

EIKON_DATA_INTERVALS = ("daily", "hourly", "minute", "tick")
EIKON_REQUEST_SIZES = {
	"daily": "year",
	"hourly": "month",
	"minute": "day",
	"tick": "hour"
}

class FixedIntervalDatabase(object):

	def __init__(self, location: str, interval: str, status: Callable[[str], None]):
		self.location: str = location
		self.interval: str = interval
		self.status = status

		self.gap = EIKON_REQUEST_SIZES[interval]

		self.rics: List[str] = []
		self.dataFrames: Dict[str, pd.DataFrame] = {}
		self.dateRanges: Dict[str, Tuple[pd.Timedelta, pd.Timestamp]] = {}

	def load_data_frame(self):
		path = os.path.join(self.location, self.interval)
		if not os.path.exists(path):
			self.status("No data for this particular sampling interval; nothing to do")
			return False

		files = sorted(os.listdir(path))

		dfs = {}

		for file in files:
			if file.startswith(".") or not file.endswith(".csv"):
				self.status(f"Skipping {file}")
				continue

			csv = os.path.join(path, file)
			self.status(f"Loading {file}")
			df = pd.read_csv(csv, parse_dates=[0], index_col=0)
			dfs[file] = df

		self.dataFrames = dfs

		return True

	def load_date_ranges(self, addRange: Callable):

		self.load_data_frame()
		dfs = self.dataFrames
		if len(dfs) == 0:
			self.status("No date ranges to load")
			return

		lastDF = dfs[sorted(dfs.keys())[-1]]
		columns = lastDF.columns

		dateRange = {}

		rics = sorted(list(set([col.split(" ")[0] for col in columns])))
		self.rics = rics

		for ric in rics:
			ricCols = [col for col in columns if col.startswith(ric + ' ')]

			firstObs = None
			lastObs = None

			for name in sorted(dfs.keys()):
				try:
					dfRic = dfs[name][ricCols].dropna(how="all")
					if dfRic.head(1).shape[0] > 0:
						firstObs = dfRic.head(1).index[0]
						break
				except Exception:
					pass

			for name in reversed(sorted(dfs.keys())):
				try:
					dfRic = dfs[name][ricCols].dropna(how="all")
					if dfRic.tail(1).shape[0] > 0:
						lastObs = dfRic.tail(1).index[0]
						break
				except Exception:
					pass

			dateRange[ric] = (firstObs, lastObs)

			addRange(ric, (firstObs, lastObs))

		self.dateRanges = dateRange
		return self.dateRanges

	# TODO: Make sure 'start' is at the beginning of the relevant period.
	# I.e. if getting daily batches of data, then make sure start is at midnight.
	def add_time_gap(self, start: pd.Timestamp):
		if self.gap == "minute":
			return (start + pd.Timedelta(minutes=1)).replace(second=0, microsecond=0)
		elif self.gap == "hour":
			return (start + pd.Timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
		elif self.gap == "day":
			return (start + pd.Timedelta(days=1)).replace(second=0, minute=0, hour=0, microsecond=0)
		elif self.gap == "month":
			if start.month < 12:
				return dt.date(start.year, start.month + 1, 1)
			else:
				return dt.date(start.year + 1, 1, 1)
		elif self.gap == "year":
			return dt.date(start.year + 1, 1, 1)

	def download_more_data(self, newRics: str = ""):

		now = pd.to_datetime("now").replace(microsecond=0)

		startDates = []
		endDates = []

		newRics = newRics.strip()

		start = None

		if len(newRics) == 0:
			ranges = self.dateRanges

			lastObservations = [ranges[ric][1] for ric in self.rics]

			if len(lastObservations) > 0:
				start = min(lastObservations)

		if not start:
			if self.interval == "daily":
				start = pd.to_datetime("1980")
			elif self.interval == "minute":
				start = now - pd.Timedelta(days=366)
			elif self.interval == "tick":
				start = now - pd.Timedelta(days=90)


		if len(newRics) > 0:
			for newRic in sorted(list(set(newRics.split(" ")))):
				self.rics.append(newRic)
				self.status(f"Adding new RIC {newRic}")

		while start < now:
			startDates.append(start)
			end = self.add_time_gap(start)
			endDates.append(end)
			start = end

		for start, end in list(zip(startDates, endDates)):

			filename = self.date_to_filename(start)
			if filename in self.dataFrames.keys():
				#self.status(f"Filename {filename} in the existing database")

				existingDF = self.dataFrames[filename]

				ricColumnInExistingDF = sorted(list(set([col.split(" ")[0] for col in existingDF.columns])))
				ricsInExistingDF = []
				for ric in ricColumnInExistingDF:
					ricCols = [col for col in existingDF.columns if col.startswith(ric + ' ')]
					ricDF = existingDF[ricCols].dropna(how="all")
					#self.status(f"ricDF shape is {ricDF.shape} made from {len(ricCols)} cols")
					if ricDF.shape[0] > 0:
						ricsInExistingDF.append(ric)

				#self.status(f"ricsInExistingDF = {ricsInExistingDF}")
				ricsToDL = [ric for ric in self.rics if ric not in ricsInExistingDF]
			else:
				#self.status(f"Filename {filename} not in the existing database")
				ricsToDL = self.rics

			if len(ricsToDL) > 0:
				if len(ricsToDL) < len(self.rics) and len(newRics) == 0:
					self.status(f"Not trying to fill in blanks in {filename}")
					continue

				self.status(f"Requesting {len(ricsToDL)} RICS to {filename} from {start} to {end} at interval '{self.interval}'")
			else:
				self.status(f"Nothing to download for {filename}")
				continue

			if EIKON_CONNECTION:

				endDate = str(end) if end < now else None

				if self.interval != "tick":
					try:
						df = ek.get_timeseries(ricsToDL, start_date=str(start), end_date=endDate, interval=self.interval)
						self.status("Downloaded new data without exception")
						try:
							self.save_chunk(filename, df)
							self.status("Saved new data without exception")
						except Exception as e:
							self.status(f"Couldn't save that data range: {e}")

					except Exception as e:
						self.status(f"Couldn't download that data range: {e}")
						self.status(f"Tried to run: ek.get_timeseries({ricsToDL}, start_date='{str(start)}', end_date='{str(end)}', interval='{self.interval}'')")

				else:

					for ric in ricsToDL:
						try:
							dfRic = ek.get_timeseries(ric, start_date=str(start), end_date=endDate, interval=self.interval)
							self.status(f"Downloaded new data for {ric} without exception")

							try:
								ricFilename = os.path.join(ric.replace('.', '-'), filename)
								self.save_chunk(ricFilename, dfRic)
								self.status("Saved new data without exception")
							except Exception as e:
								self.status(f"Couldn't save that data range: {e}")

						except Exception as e:
							self.status(f"Couldn't download that data range: {e}")
							self.status(f"Tried to run: ek.get_timeseries('{ric}', start_date='{str(start)}', end_date='{str(end)}', interval='{self.interval}'')")

	def date_to_filename(self, start: pd.Timestamp) -> str:
		if self.gap == "minute":
			start = start.replace(second=0, microsecond=0)
			filename = f"{str(start).replace(':', '-')}.csv"
		elif self.gap == "hour":
			start = start.replace(minute=0, second=0, microsecond=0)
			filename = f"{str(start).replace(':', '-')}.csv"
		elif self.gap == "day":
			filename = f"{start.date()}.csv"
		elif self.gap == "month":
			filename = f"{start.year}-{start.month}.csv"
		elif self.gap == "year":
			filename = f"{start.year}.csv"

		return filename

	def save_chunk(self, filename: str, df: pd.DataFrame):
		path = os.path.join(self.location, self.interval, filename)
		self.status(f"Saving new data to {path}")

		# Make sure the folders exist for this file to be saved
		os.makedirs(os.path.dirname(path), exist_ok=True)

		if type(df.columns) == pd.MultiIndex:
			df.columns = [' '.join(col).strip() for col in df.columns.values]
		else:
			self.status(f"Expected type of columns as MultiIndex but got {type(df.columns)}")
			self.status(f"DF name: {df.columns.name}")
			df.columns = [f"{df.columns.name} {col}" for col in df.columns]

		df = df[sorted(df.columns)]

		for col in df.columns:
			df[col] = df[col].astype("Float64")

		if os.path.exists(path):
			self.status(f"Replacing {path} data")
			backupPath = os.path.join(os.path.dirname(path), "." + os.path.basename(path))
			shutil.move(path, backupPath)

		if os.path.exists(path):
			self.status("ERROR: Should not get to here")

		df.to_csv(path)


class Window(ttk.Frame):

	def __init__(self, master: tk.Tk):
		ttk.Frame.__init__(self, master)
		self.master: tk.Tk = master

		locFrame: ttk.Frame = self.db_location()
		locFrame.pack(pady=10, padx=10)

		eikonFrame: ttk.Frame = self.eikon_and_interval()
		eikonFrame.pack(pady=10)

		addRicFrame: ttk.Frame = self.new_ric_entry()
		addRicFrame.pack(pady=10)

		summaryFrame: ttk.Frame = self.database_summary()
		summaryFrame.pack(pady=10, fill=tk.BOTH, expand=1, padx=20)

		footerFrame: ttk.Frame = self.footer()
		footerFrame.pack(fill=tk.X)

		self.pack(fill=tk.BOTH, expand=1)


	def setup_db_location(self):
		# Currently the 'askdirectory' dialog fails on MacOS Monterey
		if platform.system() != "Darwin":
			dbPath = tk.filedialog.askdirectory(initialdir=self.locationEntry.get())
		else:
			dbPath = "/Users/plaub/Dropbox/Eikon/eikon-downloader/database"

		self.update_status(f"Loading database at {dbPath}")
		self.locationEntry.delete(0, tk.END)
		self.locationEntry.insert(0, dbPath)
		self.db = FixedIntervalDatabase(self.locationEntry.get(), self.interval.get(), self.update_status)
		self.async_update_table()

	def db_location(self) -> ttk.Frame:
		locFrame = ttk.Frame(self)

		locationLabel = ttk.Label(locFrame, text="Database location:")
		locationLabel.pack(side="left")

		self.locationEntry = ttk.Entry(locFrame, width=50)
		self.locationEntry.pack(side="left", padx=10)

		defaultDBLocation = os.path.join(os.getcwd(), "database")
		print(defaultDBLocation)

		self.locationEntry.insert(0, defaultDBLocation)

		updateLocationButton = ttk.Button(locFrame, text="Change", command=self.setup_db_location)
		updateLocationButton.pack(side="left")

		return locFrame

	def update_clock(self):
		now = pd.to_datetime("now").replace(second=0, microsecond=0)
		self.time["text"] = "Time (UTC): " + str(now)
		self.after(60*1000, self.update_clock)

	def eikon_and_interval(self) -> ttk.Frame:
		eikonFrame = ttk.Frame(self)

		connLabel = ttk.Label(eikonFrame, text="Eikon status: ")
		connLabel.pack(side="left")

		connText = "Connected" if EIKON_CONNECTION else "Not connected"
		connColor = "blue" if EIKON_CONNECTION else "red"
		self.connStatus = ttk.Label(eikonFrame, text=connText, foreground=connColor)
		self.connStatus.pack(side="left", padx=10)

		intervalLabel = ttk.Label(eikonFrame, text="Data interval:")
		intervalLabel.pack(side="left", padx=10)

		self.interval = tk.StringVar()
		combobox = ttk.Combobox(eikonFrame, textvariable=self.interval, width=8)
		combobox['values'] = EIKON_DATA_INTERVALS
		combobox['state'] = 'readonly'
		combobox.current(0)
		combobox.pack(side="left")
		combobox.bind("<<ComboboxSelected>>", self.async_update_table)

		return eikonFrame

	def new_ric_entry(self) -> ttk.Frame:

		addRicFrame = ttk.Frame(self)
		ricLabel = ttk.Label(addRicFrame, text="Add new RIC:")
		ricLabel.pack(side="left")

		self.addRicEntry = ttk.Entry(addRicFrame, width=10)
		self.addRicEntry.pack(side="left", padx=10)

		updateDataButton = ttk.Button(addRicFrame, text="Add", command=self.async_request_more_data)
		updateDataButton.pack(side="left")

		return addRicFrame


	def database_summary(self) -> ttk.Frame:
		summaryFrame = ttk.Frame(self)

		self.table = ttk.Treeview(summaryFrame, columns=("RIC", "Date Range"), show="headings")

		self.table.column("# 1", anchor=tk.CENTER, width=100, stretch=tk.NO)
		self.table.heading("# 1", text="RIC")
		self.table.column("# 2", anchor=tk.CENTER)
		self.table.heading("# 2", text="Date Range")

		vsb = ttk.Scrollbar(summaryFrame, orient="vertical", command=self.table.yview)
		vsb.pack(side='right', fill='y')

		self.table.configure(yscrollcommand=vsb.set)
		self.table.pack(fill=tk.BOTH, expand=1)

		return summaryFrame

	def update_status(self, message: str):
		print(message)
		self.status["text"] = "Status: " + message

	def footer(self) -> ttk.Frame:
		footerFrame = ttk.Frame(self)

		self.status = ttk.Label(footerFrame, text="Status: ")
		self.status.pack(pady=10)

		self.time = ttk.Label(footerFrame)
		self.time.pack()
		self.update_clock()

		updateDataButton = ttk.Button(footerFrame, text="Update data", command=self.async_request_more_data)
		updateDataButton.pack(pady=10)

		return footerFrame

	def add_date_range(self, ric, dates):
		#now = pd.to_datetime("now").replace(microsecond=0)
		message = f"{dates[0]} to {dates[1]}"# + "; can download {dates[1]} to {now}"
		self.table.insert('', 'end', text="1", values=(ric, message))

	def update_table(self):
		# Clear previous output
		for i in self.table.get_children():
			self.table.delete(i)

		# Load up the range of data which is currently in the database at the current level of sampling interval
		interval = self.interval.get()
		print(f"Updating date range table for interval {interval}")

		self.db.load_date_ranges(self.add_date_range)

	def async_update_table(self, ignoreEvent=None):
		def toRun():
			self.db = FixedIntervalDatabase(self.locationEntry.get(), self.interval.get(), self.update_status)
			self.update_table()

		thread = threading.Thread(target=toRun)
		thread.start()

	def async_request_more_data(self, ignoreEvent=None):
		def toRun():
			self.db.download_more_data(self.addRicEntry.get())
			self.update_table()

		thread = threading.Thread(target=toRun)
		thread.start()



if __name__ == "__main__":

	root = tk.Tk()
	app = Window(root)
	root.wm_title("Eikon Downloader")
	root.mainloop()