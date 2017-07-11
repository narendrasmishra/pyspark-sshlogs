##
# Simple Spark ETL example
# Filter SSH auth.log files and output in CSV format
#
# CSV output looks like this:
# DATE_TIMESTR,EPOCH_TIME,EVENT,SERVER,RHOST,HOSTNAME,CLIENT_PORT,USER,MESSAGE
#
# Win Woo
##

from pyspark import SparkContext
import time
import calendar
import argparse

##
# Parse rows that look like these examples:
# Oct 31 17:23:46 14.92.177.16 sshd[8767]:  Accepted password for johnc from 67.251.40.119 port 25034 ssh2
# Aug 14 07:50:35 79.210.139.153 sshd[8163]:  Failed password for postgres from 108.105.66.12 port 24403 ssh2
##

def parse_pw(line):
    timestamp = ' '.join(line.split(" sshd")[0].split()[:3])
    epoch = str(calendar.timegm(time.strptime(''.join([timestamp, ' 2015']), "%b %d %H:%M:%S %Y")))
    ip = line.split()[3]
    rhost = line.split(" from ")[1].split()[0]
    port = line.split(" port ")[1].split()[0]
    message = line.split(": ")[1]
    event = "PASSWORD_ACCEPT" if line.split("]: ")[1].split()[0] == "Accepted" else "PASSWORD_FAIL"
    user = line.split(" for ")[1].split()[0]
    hostname = ""
    return (epoch, [timestamp, epoch, event, ip, rhost, hostname, port, user, message])

##
# Parse rows that look like these examples:
# Jul 21 21:05:18 97.220.153.186 sshd[8436]:  Accepted publickey for robertp from 84.24.194.103 port 29276 ssh2
# Jun 08 10:31:20 14.92.177.16 sshd[8122]:  Failed publickey for robertp from 67.251.40.119 port 29091 ssh2
##

def parse_ssh_key(line):
    timestamp = ' '.join(line.split(" sshd")[0].split()[:3])
    epoch = str(calendar.timegm(time.strptime(''.join([timestamp, ' 2015']), "%b %d %H:%M:%S %Y")))
    ip = line.split()[3]
    rhost = line.split(" from ")[1].split()[0]
    port = line.split(" port ")[1].split()[0]
    message = line.split(": ")[1]
    event = "PUBLICKEY_ACCEPT" if line.split("]: ")[1].split()[0] == "Accepted" else "PUBLICKEY_FAIL"
    user = line.split(" for ")[1].split()[0]
    hostname = ""
    return (epoch, [timestamp, epoch, event, ip, rhost, hostname, port, user, message])

##
# Parse rows that look like these examples:
# Feb 16 01:31:23 68.228.242.187 sshd[8427]:  Invalid user oracle from 122.19.78.65
##

def parse_invalid_user(line):
    timestamp = ' '.join(line.split(" sshd")[0].split()[:3])
    epoch = str(calendar.timegm(time.strptime(''.join([timestamp, ' 2015']), "%b %d %H:%M:%S %Y")))
    ip = line.split()[3]
    rhost = line.split(" from ")[1]
    port = ""
    message = line.split(": ")[1]
    event = "INVALID_USER"
    user =  line.split(" user ")[1].split()[0]
    hostname = ""
    return (epoch, [timestamp, epoch, event, ip, rhost, hostname, port, user, message])

##
# Parse rows that look like these examples:
# Mar 01 04:51:09 25.142.245.114 sshd[8562]:  reverse mapping checking getaddrinfo for 222-237-78-123.example.org [60.253.194.212] failed - POSSIBLE BREAK-IN ATTEMPT!
##

def parse_reverse_mapping(line):
    timestamp = ' '.join(line.split(" sshd")[0].split()[:3])
    epoch = str(calendar.timegm(time.strptime(''.join([timestamp, ' 2015']), "%b %d %H:%M:%S %Y")))
    ip = line.split()[3]
    rhost = line.split(" [")[1].split("] ")[0]
    port = ""
    message = line.split(": ")[1]
    event = "REVERSE_MAPPING_FAILED"
    user = ""
    hostname = line.split(" for ")[1].split()[0]
    return (epoch, [timestamp, epoch, event, ip, rhost, hostname, port, user, message])

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--infile", required=True, help="input file")
parser.add_argument("--outfile", required=True, help="output file")
args = parser.parse_args()

# We don't need this if running in pyspark interpreter
sc = SparkContext()
authlog = sc.textFile(args.infile)

# Password logins
password_success = authlog.filter(lambda x: "Accepted password" in x)
password_fail = authlog.filter(lambda x: "Failed password" in x)

# Logins via SSH key
ssh_key_success = authlog.filter(lambda x: "Accepted publickey" in x)
ssh_key_fail = authlog.filter(lambda x: "Failed publickey" in x)

# Other interesting events
reverse_mapping_failed = authlog.filter(lambda x: "reverse mapping checking getaddrinfo" in x)
invalid_user = authlog.filter(lambda x: "Invalid user" in x)

c1 = password_success.map(lambda x: parse_pw(x))
c2 = password_fail.map(lambda x: parse_pw(x))
c3 = ssh_key_success.map(lambda x: parse_ssh_key(x))
c4 = ssh_key_fail.map(lambda x: parse_ssh_key(x))
c5 = reverse_mapping_failed.map(lambda x: parse_reverse_mapping(x))
c6 = invalid_user.map(lambda x: parse_invalid_user(x))

# Write out the CSV to GCS
all = c1.union(c2).union(c3).union(c4).union(c5).union(c6)
#all_sorted = all.sortByKey().saveAsTextFile("gs://wwoo-hadoop-asia/out.csv")
all.map(lambda x: ''.join(['"', '","'.join(x[1]), '"'])).saveAsTextFile(args.outfile)
