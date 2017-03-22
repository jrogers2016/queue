all:
	pyinstaller -F scheduler.py
	pyinstaller -F qsub.py
	pyinstaller -F qstat.py

