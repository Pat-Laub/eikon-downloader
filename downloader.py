import tkinter as tk
import tkinter.filedialog
from tkinter import ttk # New-style widgets
import os


root = tk.Tk()

class Window(ttk.Frame):

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

	def eikon_and_frequency_frame(self):
		eikonFrame = ttk.Frame(self)

		connLabel = ttk.Label(eikonFrame, text="Eikon status: ")
		self.connStatus = ttk.Label(eikonFrame, text="Not connected", foreground="red")

		connLabel.pack(side="left")
		self.connStatus.pack(side="left", padx=10)

		frequencyLabel = ttk.Label(eikonFrame, text="Data frequency:")
		frequencyLabel.pack(side="left", padx=10)

		self.widget_var = tk.StringVar()
		combobox = ttk.Combobox(eikonFrame, textvariable=self.widget_var, width=8)
		combobox['values'] = ('daily', 'hourly', 'minute', 'tick')
		combobox['state'] = 'readonly'
		combobox.current(0)
		combobox.pack(side="left")

		return eikonFrame

	def database_summary_frame(self):
		summaryFrame = ttk.Frame(self)

		databaseLabel = ttk.Label(summaryFrame, text="Database:")
		databaseLabel.pack(side="top", pady=(0, 10))

		table = ttk.Treeview(summaryFrame, column=("RIC", "Date Range"), show="headings")

		table.column("# 1", anchor=tk.CENTER)
		table.heading("# 1", text="RIC")
		table.column("# 2", anchor=tk.CENTER)
		table.heading("# 2", text="Date Range")

		table.insert('', 'end', text="1", values=("AUD=", "1/1/10 to 2/2/20"))
		table.insert('', 'end', text="1", values=("CAD=", "1/1/10 to 2/2/20"))

		table.pack()

		return summaryFrame

	def __init__(self, master=None):
		ttk.Frame.__init__(self, master)
		self.master = master

		locFrame = self.db_location_frame()
		locFrame.pack(pady=10, padx=10)

		eikonFrame = self.eikon_and_frequency_frame()
		eikonFrame.pack(pady=10)

		summaryFrame = self.database_summary_frame()
		summaryFrame.pack(pady=10)

		updateDataButton = ttk.Button(self, text="Update data")
		updateDataButton.pack(pady=10)

		self.pack(fill=tk.BOTH, expand=1)

app = Window(root)
root.wm_title("Eikon Downloader")
root.mainloop()