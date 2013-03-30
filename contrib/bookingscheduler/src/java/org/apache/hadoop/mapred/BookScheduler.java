package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.HttpServer;

public class BookScheduler extends FairScheduler{

	static final Log LOG = LogFactory.getLog(BookScheduler.class);
	private static LRUMap finishJobStatus = new LRUMap(100, false);
	public static final String BOOKING_TIME = "mapred.bookscheduler.bookingTime";
	public static final String BOOKING_DEPENDENCY_JOBID = "mapred.bookscheduler.dependencyJobid";
	
	private boolean isWait(JobInProgress job){
		long bookingTime = job.getJobConf().getLong(BOOKING_TIME, 0);
		String[] dependencyJobs = job.getJobConf().getStrings(BOOKING_DEPENDENCY_JOBID, null);
		boolean bookingTimeFilter = false;
		boolean dependencyJobFilter = false;
		if(bookingTime >= System.currentTimeMillis()){
			bookingTimeFilter = true;
		}
		if(null != dependencyJobs){
			for(String dependencyJob : dependencyJobs){
				JobStatus dependencyJobStatus = (JobStatus) finishJobStatus.get(dependencyJob);
				if(null != dependencyJobStatus && 
						dependencyJobStatus.getRunState() != JobStatus.SUCCEEDED){
					dependencyJobFilter = true;
				}
			}
		}
		if(bookingTimeFilter || dependencyJobFilter) return true;
		else return false;
	}
	
	
	
	@Override
	public void start() {
      // Register servlet with JobTracker's Jetty server
      if (taskTrackerManager instanceof JobTracker) {
        JobTracker jobTracker = (JobTracker) taskTrackerManager;
        HttpServer infoServer = jobTracker.infoServer;
        infoServer.setAttribute("booking", this);
        infoServer.addServlet("booking", "/booking",
            BookSchedulerServlet.class);
      }
		super.start();
	}



	@Override
	protected void jobNoLongerRunning(JobInProgress job) {
		super.jobNoLongerRunning(job);
		finishJobStatus.put(job.getJobConf().getJobName(), job.getStatus());
	}

	@Override
	protected void updateRunnability() {
	    // Start by marking everything as not runnable
	    for (JobInfo info: infos.values()) {
	      info.runnable = false;
	    }
	    // Create a list of sorted jobs in order of start time and priority
	    List<JobInProgress> jobs = new ArrayList<JobInProgress>(infos.keySet());
	    Collections.sort(jobs, new FifoJobComparator());
	    // Mark jobs as runnable in order of start time and priority, until
	    // user or pool limits have been reached.
	    Map<String, Integer> userJobs = new HashMap<String, Integer>();
	    Map<String, Integer> poolJobs = new HashMap<String, Integer>();
	    for (JobInProgress job: jobs) {
	      if(isWait(job)){
	    	  LOG.debug("Booked job. It's waiting... : " + job.getJobID());
	    	  continue;	    	  
	      }
	      String user = job.getJobConf().getUser();
	      String pool = poolMgr.getPoolName(job);
	      int userCount = userJobs.containsKey(user) ? userJobs.get(user) : 0;
	      int poolCount = poolJobs.containsKey(pool) ? poolJobs.get(pool) : 0;
	      if (userCount < poolMgr.getUserMaxJobs(user) &&
	          poolCount < poolMgr.getPoolMaxJobs(pool)) {
	        if (job.getStatus().getRunState() == JobStatus.RUNNING ||
	            job.getStatus().getRunState() == JobStatus.PREP) {
	          userJobs.put(user, userCount + 1);
	          poolJobs.put(pool, poolCount + 1);
	          JobInfo jobInfo = infos.get(job);
	          if (job.getStatus().getRunState() == JobStatus.RUNNING) {
	            jobInfo.runnable = true;
	          } else {
	            // The job is in the PREP state. Give it to the job initializer
	            // for initialization if we have not already done it.
	            if (jobInfo.needsInitializing) {
	              jobInfo.needsInitializing = false;
	              jobInitializer.initJob(jobInfo, job);
	            }
	          }
	        }
	      }
	    }
	}
}
