echo "starting daily the process at: $(date '+%Y-%m-%d %H:%M:%S')"

cd ~/Desktop/work/SLA_PROBE/

source ./venv/bin/activate

cd ./SLA_GAME

/home/dungnt378/Desktop/work/SLA_PROBE/venv/bin/python3 fetch_SLA_data_daily.py -m up -d weekday