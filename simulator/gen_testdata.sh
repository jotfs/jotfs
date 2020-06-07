KiB=1024
MiB=$(expr $KiB \* $KiB)

./bin/testdata -size 1 > data/data-a
./bin/testdata -size $(expr 1 \* $KiB) > data/data-b
./bin/testdata -size $(expr 10 \* $KiB) > data/data-c
./bin/testdata -size $(expr 10 \* $KiB) > data/data-d
./bin/testdata -size $(expr 1 \* $MiB) > data/data-e
./bin/testdata -size $(expr 5 \* $MiB) > data/data-f
./bin/testdata -size $(expr 10 \* $MiB) > data/data-g
./bin/testdata -size $(expr 20 \* $MiB) > data/data-h
./bin/testdata -size $(expr 200 \* $MiB) > data/data-i