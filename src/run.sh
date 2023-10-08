#!/bin/bash

# Start the first program in the background
python ack.py &
# Get its process ID
PID1=$!

# Start the second program in the background
python client.py &
# Get its process ID
PID2=$!

# Start the third program in the background
python exchange.py &
# Get its process ID
PID3=$!

# Start the third program in the background
python market_data_streamer.py &
# Get its process ID
PID4=$!

# Wait for user input to kill the programs
read -p "Press enter to terminate the programs..."

# Kill the programs
kill $PID1 $PID2 $PID3 $PID4
