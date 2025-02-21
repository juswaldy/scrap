################################################################################
## Author:       Juswaldy Jusman
## Date Created: 2021-12-20
## Description:  Generate a list of scheduled jobs from different servers/sources.
################################################################################

import argparse
import datetime

def get_timewindow(window, interval, unit):
	"""
	Given the Window, Interval, and Unit parameters, return the begin and end times.
	"""
	# Generate times.
	now = datetime.now()
	timeformat = "%Y-%m-%d %H:%M:%S"
	datepart = now.strftime("%Y-%m-%d")
	begin_time = datetime.strptime(datepart + " 00:00:00", timeformat)
	end_time = datetime.strptime(datepart + " 23:59:59", timeformat)

	if window == "Today":
		# Begin and end times are already populated in the setup above.
		pass
	elif window == "ThisWeek":
		# From Sunday to Saturday this week.
		while begin_time.weekday() != 6:
			begin_time = begin_time - datetime.timedelta(days=1)
		while end_time.weekday() != 5:
			end_time = end_time + datetime.timedelta(days=1)
	elif window == "ThisMonth":
		# From first day to last day this month.
		begin_time = datetime.strptime(datepart + " 01", "%Y-%m-%d")
		end_time = end_time.replace(month=end_time.month + 1, day=1) - datetime.timedelta(days=1)
	elif window == "ThisHour":
		# From minute 0 to 59 of this hour.
		begin_time = datetime.strptime(datepart + " " + now.strftime("%H:00:00"), timeformat)
		end_time = datetime.strptime(datepart + " " + now.strftime("%H:59:59"), timeformat)
	elif window == "FourHours":
		# The current hour + 3 hours.
		begin_time = datetime.strptime(datepart + " " + now.strftime("%H:00:00"), timeformat)
		end_time = datetime.strptime(datepart + " " + now.strftime("%H:59:59"), timeformat)
		end_time = end_time + datetime.timedelta(hours=3)
	else:
		# Convert unit into number of seconds.
		unit_seconds = {
			"Minute": 60,
			"Hour": 60 * 60,
			"Day": 60 * 60 * 24,
			"Week": 60 * 60 * 24 * 7,
			"Month": 60 * 60 * 24 * 30,
		}[unit]

		# Get duration in seconds.
		duration = interval * unit_seconds
		duration_forward = {
			"Centered": duration / 2,
			"Forward": duration,
			"Backward": 0,
		}[window]
		duration_backward = {
			"Centered": duration / 2,
			"Forward": 0,
			"Backward": duration,
		}[window]

        # Get begin and end times.
		begin_time = datetime.strptime(now.strftime("%Y-%m-%d %H:%M:%S"), timeformat) - datetime.timedelta(seconds=duration_backward)
		end_time = datetime.strptime(now.strftime("%Y-%m-%d %H:%M:%S"), timeformat) + datetime.timedelta(seconds=duration_forward)

	return (begin_time, end_time)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate a list of scheduled jobs from different servers/sources.')
    parser.add_argument('-e', '--environment', help='Environment: Dev, Test, or Prod', required=True)
    parser.add_argument('-s', '--server', help='Server: All (default), Jenzabar, Aqueduct, FinancialReporting, CloverDX, Cron1Services, Cron1Tasks', default='All', required=False)
    parser.add_argument('-u', '--unit', help='Unit: Minute, Hour (default), Day, Week, Month', default='Hour', required=False)
    parser.add_argument('-i', '--interval', help='How many minutes/hours/days/weeks/months are we going to look at? Default is 1', default=1 required=False)
    parser.add_argument('-r', '--run', help='Task to run', required=True)
    args = parser.parse_args()
