# Need to change here to get right path of libcodes.so.0
LD_LIBRARY_PATH=$HOME/Goethe_Uni/9_SoSe23/Masterarbeit/codes-dev/build-codes/lib
export LD_LIBRARY_PATH

#mpirun -n 2 ./example_jh --synch=3 --codes-config=example_jh.conf --stripe-size=4 --stripe-count=5
#mpirun --use-hwthread-cpus ./example_jh --synch=3 --codes-config=example_jh.conf -extramem=100000 --stripe-size=4 --stripe-count=5
mpirun -n 2 ./example_jh --synch=3 --codes-config=example_jh.conf --stripe-size=4 --stripe-count=5
