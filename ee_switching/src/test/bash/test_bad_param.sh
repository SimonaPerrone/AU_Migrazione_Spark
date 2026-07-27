#die "Bad storici param : you cannot pass -t and ( any param)"
./ee-switching-dati-storici.sh --debug -t './app-data/file_input/timestamp.txt' -p './app-data/file_input/sw_dates.txt'
#die "Bad storici param : you cannot pass -s and ( -p and -d and -D )"
./ee-switching-dati-storici.sh --debug -s './app-data/file_input/sw_dates.txt' -p './app-data/file_input/sw_dates.txt' -D './app-data/file_input/sw_dates.txt'
#die "Bad storici param : you cannot pass -s and (-d and -D )"
./ee-switching-dati-storici.sh --debug -s './app-data/file_input/sw_dates.txt' -d './app-data/file_input/pod_list.txt' -D './app-data/file_input/pod_list.txt'
#die "Bad storici param : you cannot pass -s and (-p and -d )"
./ee-switching-dati-storici.sh --debug -s './app-data/file_input/sw_dates.txt' -p './app-data/file_input/pod_list.txt' -d './app-data/file_input/pod_list.txt'
#die "Bad storici param : you cannot pass -s and (-p and -D )"
./ee-switching-dati-storici.sh --debug -s './app-data/file_input/sw_dates.txt' -p './app-data/file_input/pod_list.txt' -D './app-data/file_input/pod_list.txt'

