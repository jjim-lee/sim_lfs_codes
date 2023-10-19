# Update the following 2 variables for your own system:
CODES=/home/jinhyuk/Goethe_Uni/9_SoSe23/Masterarbeit/codes-dev/codes-1.4.2/build/
ROSS=/home/jinhyuk/Goethe_Uni/9_SoSe23/Masterarbeit/codes-dev/build-ross/


ifndef CODES

$(error CODES is undefined, see README)

  endif

  ifndef ROSS

  $(error ROSS is undefined, see README)

  endif

override CPPFLAGS += $(shell $(ROSS)/bin/ross-config --cflags) -I$(CODES)/include
CC = $(shell $(ROSS)/bin/ross-config --cc)
  LDFLAGS = $(shell $(ROSS)/bin/ross-config --ldflags) -L$(CODES)/lib
  LDLIBS = $(shell $(ROSS)/bin/ross-config --libs) -lcodes




 example_jh: example_jh.c



 clean: rm -f example_jh
