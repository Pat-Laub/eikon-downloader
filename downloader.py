
# Packages required for the view/GUI
import tkinter as tk
import tkinter.filedialog
from tkinter import ttk # New-style widgets
import os
import threading

# Packages required for the model/data-wrangling
import dask.dataframe as dd
import pandas as pd
import datetime as dt

# Setup for downloading Eikon data
try:
	import configparser as cp

	cfg = cp.ConfigParser()
	cfg.read('eikon.cfg')  # adjust for different file location
	print(f"Eikon app id is {cfg['eikon']['app_id']}")

	import eikon as ek

	ek.set_app_key(cfg['eikon']['app_id'])
	print("Connected to Eikon Data API!")
	EIKON_CONNECTION = True

	import logging
	eikonLogger = logging.getLogger('pyeikon')
	eikonLogger.setLevel(logging.FATAL)

except:
	print("Exception when trying to connect to Eikon Data API")
	EIKON_CONNECTION = False

EIKON_DATA_FREQUENCIES = ("daily", "hourly", "minute", "tick")
EIKON_REQUEST_SIZES = {
	"daily": "year",
	"hourly": "month",
	"minute": "day",
	"tick": "minute"
}

# TODO: Make sure 'start' is at the beginning of the relevant period.
# I.e. if getting daily batches of data, then make sure start is at midnight.
def add_time_gap(start, gap):
	if gap == "minute":
		return (start + pd.Timedelta(minutes=1)).replace(second=0, microsecond=0)
	elif gap == "day":
		return (start + pd.Timedelta(days=1)).replace(second=0, minute=0, hour=0, microsecond=0)
	elif gap == "month":
		if start.month < 12:
			return dt.date(start.year, start.month + 1, 1)
		else:
			return dt.date(start.year + 1, 1, 1)
	elif gap == "year":
		return dt.date(start.year + 1, 1, 1)

	return end

class Database(object):

	def __init__(self, location):

		self.location = location
		self.rics = {}
		self.dataFrames = {}
		self.dateRanges = {}

	def load_data_frame(self, freq):
		path = os.path.join(self.location, freq)
		if not os.path.exists(path):
			print("No data for this particular sampling frequency; nothing to do")
			return False

		files = os.listdir(path)

		dates = [name.split('.')[0] for name in sorted(files)]
		dates = list(pd.to_datetime(dates))
		dates.append(dates[-1] + pd.Timedelta(days=1))

		csv = os.path.join(path, "*.csv")
		print(f"Loading {csv}")
		df = dd.read_csv(csv, parse_dates=[0])
		df = df.set_index("Date", sorted=True, divisions=dates)

		self.dataFrames[freq] = df
		return True

	def load_date_ranges(self, freq, addRange):

		if freq in self.dateRanges:
			return self.dateRanges[freq]

		if freq not in self.dataFrames:
			if not self.load_data_frame(freq):
				return
		df = self.dataFrames[freq]

		dateRange = {}

		rics = sorted(list(set([col.split(" ")[0] for col in df.columns])))
		self.rics[freq] = rics

		for ric in rics:
			ricCols = [col for col in df.columns if col.startswith(ric + ' ')]
			dfRic = df[ricCols].dropna()

			if dfRic.head(1).shape[0] > 0 and dfRic.tail(1).shape[0] > 0:
				firstObs = dfRic.head(1).index[0]
				lastObs = dfRic.tail(1).index[0]
			else:
				PRECISE = True
				if PRECISE:
					# Convert to a pandas dataframe (slow!)
					dfRic = dfRic.compute()
					firstObs = dfRic.index[0]
					lastObs = dfRic.index[-1]
				else:
					firstObs = None
					lastObs = None

			dateRange[ric] = (firstObs, lastObs)
			print(f"Ric {ric} observations from {firstObs} to {lastObs}")

			addRange(ric, (firstObs, lastObs))

		self.dateRanges[freq] = dateRange
		return self.dateRanges[freq]

	def download_more_data(self, freq):

		now = pd.to_datetime("now").replace(microsecond=0)

		startDates = []
		endDates = []

		gap = EIKON_REQUEST_SIZES[freq]

		ranges = self.dateRanges[freq]

		lastObservations = [ranges[ric][1] for ric in self.rics[freq]]
		start = min(lastObservations)

		while start < now:
			startDates.append(str(start))
			end = add_time_gap(start, gap)
			endDates.append(str(end))
			start = end

		for start, end in list(zip(startDates, endDates)):
			print(f"Requesting {len(self.rics[freq])} RICS from {start} to {end}")
			try:
				if EIKON_CONNECTION:
					df = ek.get_timeseries(self.rics[freq], start_date = str(start), end_date = str(end), interval="minute")
					self.save_chunk(freq, start, df)
			except Exception:
				print("Couldn't get that data range")
				pass

	def save_chunk(self, freq, start, df):
		# TODO: Check if this potentially '@staticmethod' should be written as one.
		gap = EIKON_REQUEST_SIZES[freq]

		if gap == "minute":
			start = start.replace(second=0, microsecond=0)
			filename = f"{str(start).replace(':', '-')}.csv"
		elif gap == "day":
			filename = f"{start.date()}.csv"
		elif gap == "month":
			filename = f"{start.year}-{start.month}.csv"
		elif gap == "year":
			filename = f"{start.year}.csv"

		path = os.path.join(freq, filename)

		if os.path.exists(path):
			print(f"Replacing {path} data")
			os.remove(path)

		df.to_csv(path)


db = Database("/Users/plaub/Dropbox/Eikon/eikon-downloader/database")

class Window(ttk.Frame):

	def __init__(self, master=None):
		ttk.Frame.__init__(self, master)
		self.master = master

		locFrame = self.db_location_frame()
		locFrame.pack(pady=10, padx=10)

		eikonFrame = self.eikon_and_frequency_frame()
		eikonFrame.pack(pady=10)

		summaryFrame = self.database_summary_frame()
		summaryFrame.pack(pady=10, fill=tk.BOTH, expand=1, padx=20)

		footerFrame = self.footer_frame()
		footerFrame.pack(fill=tk.X)

		self.pack(fill=tk.BOTH, expand=1)

		self.async_update_table()

	def setup_db_location(self):
		# Currently the 'askdirectory' dialog fails on MacOS beta
		x = tk.filedialog.askdirectory(initialdir=self.location.get())
		print(x)
		self.location.insert(0, x)

	def db_location_frame(self):
		locFrame = ttk.Frame(self)

		locationLabel = ttk.Label(locFrame, text="Database location:")
		locationLabel.pack(side="left")

		self.location = ttk.Entry(locFrame, width=50)
		self.location.pack(side="left", padx=10)

		defaultDBLocation = os.path.join(os.getcwd(), "database")
		print(defaultDBLocation)

		self.location.insert(0, defaultDBLocation)

		updateLocationButton = ttk.Button(locFrame, text="Change", command=self.setup_db_location)
		updateLocationButton.pack(side="left")

		return locFrame

	def update_clock(self):
		now = pd.to_datetime("now").replace(microsecond=0)
		self.time["text"] = "Time (UTC): " + str(now)
		self.after(1000, self.update_clock)

	def eikon_and_frequency_frame(self):
		eikonFrame = ttk.Frame(self)

		connLabel = ttk.Label(eikonFrame, text="Eikon status: ")
		connLabel.pack(side="left")

		self.connStatus = ttk.Label(eikonFrame, text="Not connected", foreground="red")
		self.connStatus.pack(side="left", padx=10)

		frequencyLabel = ttk.Label(eikonFrame, text="Data frequency:")
		frequencyLabel.pack(side="left", padx=10)

		self.frequency = tk.StringVar()
		combobox = ttk.Combobox(eikonFrame, textvariable=self.frequency, width=8)
		combobox['values'] = EIKON_DATA_FREQUENCIES
		combobox['state'] = 'readonly'
		combobox.current(2)
		combobox.pack(side="left")
		combobox.bind("<<ComboboxSelected>>", self.async_update_table)

		return eikonFrame

	def database_summary_frame(self):
		summaryFrame = ttk.Frame(self)

		databaseLabel = ttk.Label(summaryFrame, text="Database:")
		databaseLabel.pack(side="top", pady=(0, 10))

		self.table = ttk.Treeview(summaryFrame, column=("RIC", "Date Range"), show="headings")

		self.table.column("# 1", anchor=tk.CENTER, width=100, stretch=tk.NO)
		self.table.heading("# 1", text="RIC")
		self.table.column("# 2", anchor=tk.CENTER)
		self.table.heading("# 2", text="Date Range")

		vsb = ttk.Scrollbar(summaryFrame, orient="vertical", command=self.table.yview)
		vsb.pack(side='right', fill='y')

		self.table.configure(yscrollcommand=vsb.set)
		self.table.pack(fill=tk.BOTH, expand=1)

		return summaryFrame

	def footer_frame(self):
		footerFrame = ttk.Frame(self)

		self.time = ttk.Label(footerFrame)
		self.time.pack()
		self.update_clock()

		updateDataButton = ttk.Button(footerFrame, text="Update data", command=lambda: db.download_more_data(self.frequency.get()))
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

		# Load up the range of data which is currently in the database at the current level of sampling frequency
		freq = self.frequency.get()
		print(f"Updating date range table for frequency {freq}")

		db.load_date_ranges(freq, self.add_date_range)

	def async_update_table(self, ignoreEvent=None):
		thread = threading.Thread(target=self.update_table)
		thread.start()


if __name__ == "__main__":
	root = tk.Tk()
	app = Window(root)
	root.wm_title("Eikon Downloader")
	root.mainloop()