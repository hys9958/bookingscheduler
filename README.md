bookingscheduler
================

### Note : MapReduce booking job scheduler. It's be based on fair scheduler of apache hadoop. ###

- BookingScheduler is M/R scheduler of Hadoop Jobtracker. You can book a M/R job by bookingScheduler.
- BookingScheduler는 apache hadoopd의 jobtracker에서 사용되는 스케줄러로써, 기존 fairScheduler을 기반으로 만들어졌다. 
그러므로 모든 기능과 설정은 fairScheduler과 동일하고, 여기에 job 예약하는 기능이 추가되어 있다.

- job 예약은 2가지로 할 수 있다.
1 booking time을 설정하여 설정한 시간이 되면 job이 실행.
2 dependency job id를 설정하여 설정한 job에 success되면 job이 실행.

## Screen shot ##
![Settings Window](https://raw.github.com/hys9958/bookingscheduler/master/contrib/bookingscheduler/designdoc/bookingScheduler.png)

## Compiling ##
- compiling requires : 
	- java JDK 1.6,
	- contrib apache hadoop-1.0.X
	- ant
- dependency
	- apache hadoop-1.0.X
	
## Install ##
1 hadoop-bookingscheduler-${version}.jar moves to $HADOOP_HOME/lib.
2 Append to $HADOOP_HOME/conf/mapred-site.xml

	<property>
  		<name>mapred.jobtracker.taskScheduler</name>
  		<value>org.apache.hadoop.mapred.BookScheduler</value>
	</property>
    	
3 setting fair-scheduler.xml
4 restart jobtracker

## usage ##
M/R client programming.
	
	Configuration conf = new Configuration();
    conf.setLong(BookScheduler.BOOKING_TIME, {TimeMillis});

OR

    conf.setStrings(BookScheduler.BOOKING_DEPENDENCY_JOBID, new String[]{"job_201303301602_0003"});

## web manager page ##
- http://hadoop:50030/booking

hanyounsu@gmail.com