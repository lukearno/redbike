mkdir -p benchmark/results
JOBS=1000000

python benchmark/benchmark.py $JOBS 2
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(2)' &> benchmark/results/$JOBS-2x2.0.out &
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(2)' &> benchmark/results/$JOBS-2x2.1.out &
sleep 60
redbike -c benchmark/benchmark.conf control HALT
wc -l benchmark/results/$JOBS-2x2.*.out

sleep 20

python benchmark/benchmark.py $JOBS 4
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(4)' &> benchmark/results/$JOBS-4x2.0.out &
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(4)' &> benchmark/results/$JOBS-4x2.1.out &
sleep 60
redbike -c benchmark/benchmark.conf control HALT
wc -l benchmark/results/$JOBS-4x2.*.out

sleep 20

python benchmark/benchmark.py $JOBS 2
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(2)' &> benchmark/results/$JOBS-2x4.0.out &
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(2)' &> benchmark/results/$JOBS-2x4.1.out &
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(2)' &> benchmark/results/$JOBS-2x4.2.out &
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(2)' &> benchmark/results/$JOBS-2x4.3.out &
sleep 60
redbike -c benchmark/benchmark.conf control HALT
wc -l benchmark/results/$JOBS-2x4.*.out

sleep 20

python benchmark/benchmark.py $JOBS 4
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(4)' &> benchmark/results/$JOBS-4x4.0.out &
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(4)' &> benchmark/results/$JOBS-4x4.1.out &
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(4)' &> benchmark/results/$JOBS-4x4.2.out &
redbike -c benchmark/benchmark.conf work 'benchmark.benchmark:Worker(4)' &> benchmark/results/$JOBS-4x4.3.out &
sleep 60
redbike -c benchmark/benchmark.conf control HALT
wc -l benchmark/results/$JOBS-4x4.*.out


