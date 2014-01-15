import sys
import time
import subprocess
from collections import defaultdict
from subprocess import Popen, PIPE, STDOUT

richpush_ualoggrep = "ua-loggrep -s richpush-v2 -b 2014-01-14 -e 2014-01-14 -r '.*N8PAcVorRNeDC1YkyOPRaA.*'"

last_richpush_count = 0

max_richpush_reached_count = 0

#Timing loop
while True:

        p = subprocess.Popen(richpush_ualoggrep, shell=True, stdin=PIPE, stdout=PIPE)

        #Sometimes this script needs a little motivation to continue
        p.stdin.write('y')
        p.stdin.close()


        while True:
                line = p.stdout.readline()
                if line == '' and p.poll() != None:
                        break

                sys.stdout.write(line)

                # After we're all done with the ualoggrep, it gets concactinated, find the count
                concatinate_text = 'Concatenated all results to'
                concatinate_find = line.find(concatinate_text)

                if concatinate_find == -1:
                        continue
                concatinate_length = len(concatinate_text)

                ualoggrep_loc = line[concatinate_length:]

                ualoggrep_loc = ualoggrep_loc.strip(' \n')


        message_user_map = defaultdict(list)

        f = open(ualoggrep_loc, 'r')

        for line in f:
            # Make sure it's a completed line
            completed_index = line.find("Completed: SendMessageJob{messageId='")

            if completed_index == -1:
                continue

            # Get the message_id
            message_id_start = completed_index + 37
            message_id_end = line.find("'", message_id_start)

            message_id = line[message_id_start:message_id_end]

            # Get user_ids for the message
            user_index = line.find('userIds=[')

            if user_index == -1:
                continue

            open_bracket = user_index + 8
            close_bracket = line.find(']', open_bracket)

            user_ids = line[open_bracket:close_bracket].split(',')

            # Add these users to the message_id
            message_user_map[message_id].extend(user_ids)

        total = 0
        for message_id, user_ids in message_user_map.iteritems():
            print "Message Id:", message_id, "Send Count:", len(user_ids)
            total += len(user_ids)

        print "Total Send Count:", total

        if last_richpush_count == total:
                print "**********\n\n\n REACHED MAX RICHCOUNT \n\n\n **********"

                max_richpush_reached_count += 1

                if max_richpush_reached_count == 2:
                        print "**********\n\n\n MAX RICHCOUNT OF " , total , " CONFIRMED \n\n\n **********"
                        break

        last_richpush_count = total

        print "**********\n\n\n WAITING \n\n\n **********"

        time.sleep(1800)

