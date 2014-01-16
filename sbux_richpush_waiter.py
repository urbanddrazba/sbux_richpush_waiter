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
        message_time_first_created_map = defaultdict(list)
        message_time_last_created_map = defaultdict(list)
        message_time_first_sent_map = defaultdict(list)
        message_time_last_sent_map = defaultdict(list)

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

            # Get the timestamp at which the message was first created

            created_at_index = line.find('createdAt=')
            created_at_start = created_at_index + 11
            created_at_end = line.find("'", created_at_start)

            message_created_at = line[created_at_start:created_at_end]

            if not message_time_first_created_map[message_id] or message_time_first_created_map[message_id] > message_created_at:
                message_time_first_created_map[message_id] = message_created_at

            if not message_time_last_created_map[message_id] or message_time_last_created_map[message_id] < message_created_at:
                message_time_last_created_map[message_id] = message_created_at

            # Get the timestamp at which the message was last sent
            timestamp_index = line.find("INFO")
            timestamp_start = timestamp_index - 26
            timestamp_end = line.find(',', timestamp_start)

            message_timestamp = line[timestamp_start:timestamp_end]

            if not message_time_first_sent_map[message_id] or message_time_first_sent_map[message_id] > message_timestamp:
                message_time_first_sent_map[message_id] = message_timestamp

            if not message_time_last_sent_map[message_id] or message_time_last_sent_map[message_id] < message_timestamp:
                message_time_last_sent_map[message_id] = message_timestamp

        total = 0

        for message_id, user_ids in message_user_map.iteritems():
            print "\n\nMessage Id:", message_id, "Send Count:", len(user_ids)
            print "Message First Created:", message_time_first_created_map[message_id] , "Message Last Created:" , message_time_last_created_map[message_id], "First sent:", message_time_first_sent_map[message_id], "Last sent:", message_time_last_sent_map[message_id]
            total += len(user_ids)


        print "\n\nNow it is " , (time.strftime("%H:%M:%S"))

        print "Total Send Count:", total

        if last_richpush_count == total and last_rishpush_count != 0:
                print "**********\n\n\n REACHED MAX RICHCOUNT \n\n\n**********"

                max_richpush_reached_count += 1

                if max_richpush_reached_count == 2:
                        print "**********\n\n\n MAX RICHCOUNT OF " , total , " CONFIRMED \n\n\n**********"
                        break

        last_richpush_count = total

        print "**********\n\n\n WAITING \n\n\n**********"

        time.sleep(1800) #half hour


