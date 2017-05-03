##
# Generates a simulated SSH log
#
# Win Woo
##

import random
import time

good_rhost = []
bad_rhost = []
server_ip = []
templates = []
good_user = ["johnc", "carlk", "billm", "jillp", "mariak", "miles", "careym", "robertp"]
#bad_user = ["root", "admin", "guest", "test", "a", "user", "hadoop", "postgres", "webmaster", "mysql", "johnc", "carlk", "billm"]
bad_user = ["root", "admin", "guest", "test", "a", "user", "hadoop", "postgres", "webmaster", "mysql"]
invalid_user = ["oracle", "samba"]
rhostname = ["222-237-78-139.example.org", "222-237-78-123.example.org", "222-236-63-128.example.org"]

min_time = 1388613094
max_time = 1417470694

def generate_time():
    return random.randint(min_time, max_time)


def generate_ip():
    CLASSES='A'
    ip_class = random.choice(CLASSES)

    if ip_class == "A":
        first_octet = random.randint(1,126)

    if ip_class == "B":
        first_octet = random.randint(128,191)

    if ip_class == "C":
        first_octet = random.randint(192,223)

    second_octet = random.randint(0,254)
    third_octet = random.randint(0,254)
    fourth_octet = random.randint(0,254)
    return "%i.%i.%i.%i" %(first_octet,second_octet,third_octet,fourth_octet)

for x in range(0, 40):
    good_rhost.append(generate_ip())

for x in range(0, 7):
    bad_rhost.append(generate_ip())

for x in range(0, 10):
    server_ip.append(generate_ip())

with open('template.txt') as f:
    templates = f.read().splitlines()

random.seed()

m = len(templates)

for x in range (0, 20000000):
    line = templates[random.randint(0, m-1)]
    line = line.replace("[TIME]", time.strftime("%b %d %H:%M:%S", time.localtime(generate_time())))
    line = line.replace("[HOST]", server_ip[random.randint(0, len(server_ip)-1)])


    line = line.replace("[RHOST]", good_rhost[random.randint(0, len(good_rhost)-1)])
    line = line.replace("[USER]", good_user[random.randint(0, len(good_user)-1)])

    line = line.replace("[BAD_RHOST]", bad_rhost[random.randint(0, len(bad_rhost)-1)])
    line = line.replace("[BAD_USER]", bad_user[random.randint(0, len(bad_user)-1)])

    line = line.replace("[PID]", ''.join(['sshd[', str(random.randint(8000, 9000)), ']: ']))
    line = line.replace("[PORT]", str(random.randint(20000, 30000)))
    line = line.replace("[RHOSTNAME]", rhostname[random.randint(0, len(rhostname)-1)])
    line = line.replace("[INVALIDUSER]", invalid_user[random.randint(0, len(invalid_user)-1)])

    print line
