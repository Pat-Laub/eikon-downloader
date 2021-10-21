
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
		self.path = os.path.join(location, interval)
		self.interval: str = interval
		self.status = status

		self.gap = EIKON_REQUEST_SIZES[interval]


		# Read through the subdirectories to see which RICs we already have in this database
		self.rics: List[str] = []
		self.dateRanges: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]] = {}

		os.makedirs(self.path, exist_ok=True) # Create the directory if required
		self.load()

	def load(self) -> None:
		self.rics: List[str] = []

		self.dateRanges: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]] = {}

		subdirs = sorted(os.listdir(self.path))

		for ricName in subdirs:
			ricPath = os.path.join(self.path, ricName)

			if os.path.isdir(ricPath):
				ric = ricName.replace('-', '.')
				self.rics.append(ric)

				#self.status(f"Looking for csv's in {ricPath}")
				csvs = os.listdir(ricPath)
				csvs = [csv for csv in csvs if csv.endswith(".csv") and not csv.startswith(".") and os.path.getsize(os.path.join(ricPath, csv)) > 0]
				csvs = list(sorted(csvs))

				if len(csvs) > 0:
					## The fast version is just to read the filenames to find the date ranges of the existing data
					#firstDate = pd.to_datetime(csvs[0].split(".")[0])
					#lastDate = pd.to_datetime(csvs[-1].split(".")[0])

					firstCSV = os.path.join(ricPath, csvs[0])
					firstDF = pd.read_csv(firstCSV, parse_dates=[0], index_col=0)
					firstDate = firstDF.index[0]

					lastCSV = os.path.join(ricPath, csvs[-1])
					lastDF = pd.read_csv(lastCSV, parse_dates=[0], index_col=0)
					lastDate = lastDF.index[-1]

					self.dateRanges[ric] = (firstDate, lastDate)

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

	def add_new_rics(self, newRics: str):

		newRics = newRics.strip()
		newRics = [] if newRics == "" else newRics.split(" ")

		for ric in newRics:
			self.rics.append(ric)
			ricFolder = os.path.join(self.path, ric.replace('.', '-'))
			os.makedirs(ricFolder, exist_ok=True) # Create the directory if required

		self.rics = list(sorted(self.rics))

	def download_more_data(self, selectedRics: List[str] = []):
		if not EIKON_CONNECTION:
			return

		# Set start date as far back as possible.
		now = pd.to_datetime("now").replace(microsecond=0)
		if self.interval == "daily":
			start = pd.to_datetime("1980")
		elif self.interval == "minute":
			start = now - pd.Timedelta(days=366)
		elif self.interval == "tick":
			start = now - pd.Timedelta(days=90)

		# Precompute the start/end periods which will be requested; useful later for progress bars.
		startDates = []
		endDates = []
		while start < now:
			end = self.add_time_gap(start)
			startDates.append(start)
			endDates.append(end)
			start = end

		ricsToDownload = selectedRics if len(selectedRics) > 0 else self.rics

		for start, end in zip(startDates, endDates):
			incomplete = pd.to_datetime("now") < end
			filename = self.date_to_filename(start, incomplete)

			if len(ricsToDownload) == 1:
				self.status(f"Requesting RIC {ricsToDownload[0]} from {start} to {end} at interval '{self.interval}'")
			else:
				self.status(f"Requesting {len(ricsToDownload)} RICS from {start} to {end} at interval '{self.interval}'")

			for ric in ricsToDownload:
				ricFilename = os.path.join(ric.replace('.', '-'), filename)
				ricPath = os.path.join(self.path, ricFilename)
				if os.path.exists(ricPath) and "incomplete" not in filename:
					self.status(f"Skipping over existing data in {ricFilename}")
					continue

				self.status(f"Requesting data for {ricFilename}")
				try:
					endDate = str(end) if end < now else None
					dfRic = ek.get_timeseries(ric, start_date=str(start), end_date=endDate, interval=self.interval)
					dfRic = dfRic.dropna(how="all")
					saveDF = True
				except Exception as e:
					self.status(f"Couldn't download that data range: {e}")

					if 'code' in dir(e) and e.code == -1:
						# When Eikon gives us the error "No data available for the requested date range" then
						# we can create an empty file to signify that we tried this request, and we need not try again later.
						dfRic = pd.DataFrame()
						saveDF = True
					else:
						saveDF = False

				if saveDF:
					try:
						self.save_chunk(ricFilename, dfRic)
					except Exception as e:
						self.status(f"Couldn't save that data range: {e}")

	def date_to_filename(self, start: pd.Timestamp, incomplete: bool) -> str:
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

		if incomplete:
			filename = filename.replace(".csv", ".incomplete.csv")

		return filename

	def save_chunk(self, filename: str, df: pd.DataFrame):
		path = os.path.join(self.path, filename)
		self.status(f"Saving new data to {path}")

		if df.shape[0] > 0:
			if type(df.columns) == pd.MultiIndex:
				# This shouldn't happen now we request each RIC individually.
				df.columns = [' '.join(col).strip() for col in df.columns.values]
			else:
				df.columns = [f"{df.columns.name} {col}" for col in df.columns]

			df = df[sorted(df.columns)]

			for col in df.columns:
				df[col] = df[col].astype("Float64")

		if os.path.exists(path):
			self.status(f"Replacing {path} data")
			backupPath = os.path.join(os.path.dirname(path), "." + os.path.basename(path))
			shutil.move(path, backupPath)

		if df.shape[0] > 0:
			df.to_csv(path)
		else:
			# Pandas will add spurious quotations when trying to save an empty dataframe,
			# so instead we create an empty file manually.
			f = open(path, "w")
			f.close()


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

		self.load_database()

	def select_new_database(self):
		# Currently the 'askdirectory' dialog fails on MacOS Monterey
		if platform.system() != "Darwin":
			dbPath = tk.filedialog.askdirectory(initialdir=self.locationEntry.get())
		else:
			dbPath = "/Users/plaub/Dropbox/Eikon/eikon-downloader/database"

		self.locationEntry.delete(0, tk.END)
		self.locationEntry.insert(0, dbPath)

		self.load_database()

	def load_database(self):
		self.update_status(f"Loading database at {self.locationEntry.get()}")
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

		updateLocationButton = ttk.Button(locFrame, text="Change", command=self.select_new_database)
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

		def new_ric():
			self.db.add_new_rics(self.addRicEntry.get())
			self.load_database()

		addRicButton = ttk.Button(addRicFrame, text="Add", command=new_ric)
		addRicButton.pack(side="left")

		return addRicFrame


	def database_summary(self) -> ttk.Frame:
		summaryFrame = ttk.Frame(self)

		self.table = ttk.Treeview(summaryFrame, columns=("RIC", "Date Range"), show="headings")

		self.table.column("# 1", anchor=tk.CENTER, width=100, stretch=tk.NO)
		self.table.heading("# 1", text="RIC")
		self.table.column("# 2", anchor=tk.CENTER)
		self.table.heading("# 2", text="Date Range")

		def enable_update_selected_button(event=None):
			if len(self.table.selection()) > 0:
				self.updateSelectedButton["state"] = tk.NORMAL

		self.table.bind('<ButtonRelease-1>', enable_update_selected_button)

		vsb = ttk.Scrollbar(summaryFrame, orient="vertical", command=self.table.yview)
		vsb.pack(side='right', fill='y')

		self.table.configure(yscrollcommand=vsb.set)
		self.table.pack(fill=tk.BOTH, expand=1)

		return summaryFrame

	def update_status(self, message: str):
		print(message)
		self.statusLabel["text"] = "Status: " + message

	def footer(self) -> ttk.Frame:
		footerFrame = ttk.Frame(self)

		self.statusLabel = ttk.Label(footerFrame, text="Status: ")
		self.statusLabel.pack(pady=10)

		self.time = ttk.Label(footerFrame)
		self.time.pack()
		self.update_clock()

		updateButtonsFrame = ttk.Frame(footerFrame)

		def update_selected():
			selectedRics = [self.table.item(item)['values'][0] for item in self.table.selection()]
			print(f"{selectedRics=}")
			self.async_request_more_data(selectedRics=selectedRics)

		self.updateSelectedButton = ttk.Button(updateButtonsFrame, text="Update Selected", command=update_selected)
		self.updateSelectedButton["state"] = tk.DISABLED
		self.updateSelectedButton.pack(side="left")

		updateAllButton = ttk.Button(updateButtonsFrame, text="Update All", command=self.async_request_more_data)
		updateAllButton.pack(side="left")

		updateButtonsFrame.pack(pady=10)

		return footerFrame

	def update_table(self):
		# Clear previous output
		for i in self.table.get_children():
			self.table.delete(i)

		self.updateSelectedButton["state"] = tk.DISABLED

		self.db.load()

		for ric in self.db.rics:
			if ric in self.db.dateRanges.keys():
				dates = self.db.dateRanges[ric]
				message = f"{dates[0]} to {dates[1]}"# + "; can download {dates[1]} to {now}"
				self.table.insert('', 'end', text="1", values=(ric, message))
			else:
				message = f"No data"
				self.table.insert('', 'end', text="1", values=(ric, message))

		self.update_status("Database loaded")

	def async_update_table(self, ignoreEvent=None):
		def toRun():
			self.db = FixedIntervalDatabase(self.locationEntry.get(), self.interval.get(), self.update_status)
			self.update_table()

		thread = threading.Thread(target=toRun)
		thread.start()

	def async_request_more_data(self, ignoreEvent=None, selectedRics=[]):
		def toRun():
			self.db.download_more_data(selectedRics)
			self.update_table()

		thread = threading.Thread(target=toRun)
		thread.start()



if __name__ == "__main__":

	root = tk.Tk()
	app = Window(root)
	root.wm_title("Eikon Downloader")
	root.mainloop()
