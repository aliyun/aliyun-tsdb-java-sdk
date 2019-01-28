package com.aliyun.hitsdb.client.value.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.aliyun.hitsdb.client.value.JSONValue;

public class Query extends JSONValue {
	private Long start;
	private Long end;
	private Boolean msResolution;
	private Boolean delete;
	private List<SubQuery> queries;

	public static class Builder {
		private Long startTime;
		private Long endTime;
		private Boolean msResolution;
		private Boolean delete;
		private List<SubQuery> subQueryList = new ArrayList<SubQuery>();

		/**
		 * 1970-02-20 00:59:28
		 */
		private static final long MIN_START_TIME = 4284768;
		/**
		 * 2286-11-21 01:46:39.999
		 */
		private static final long MAX_END_TIME = 9999999999999L;


		public Builder(long startTime) {
			this.startTime = startTime;
		}

		public Builder(long startTime, long endTime) {
			this.startTime = startTime;
			this.endTime = endTime;
		}
		
		public Builder end(Date endDate) {
            this.endTime = endDate.getTime();
            return this;
        }

		public Builder end(long endTime) {
			this.endTime = endTime;
			return this;
		}

		public Builder msResolution(Boolean msResolution) {
			if (msResolution) {
				this.msResolution = true;
			} else {
				this.msResolution = null;
			}
			return this;
		}

		public Builder msResolution() {
			this.msResolution = true;
			return this;
		}

		public Builder sub(SubQuery... subQuerys) {
			int newIndex = subQueryList.size();
			int i = 0;
			for (SubQuery subQuery : subQuerys) {
				if (subQuery.getIndex() <= 0) {
					subQuery.setIndex(newIndex + i);
				}
				subQuery.setIndex(newIndex + i);
				subQueryList.add(subQuery);
				i++;
			}
			return this;
		}

		public Builder sub(Collection<SubQuery> subQuerys) {
			int newIndex = subQueryList.size();
			int i = 0;
			for (SubQuery subQuery : subQuerys) {
				if (subQuery.getIndex() <= 0) {
					subQuery.setIndex(newIndex + i);
				}
				subQueryList.add(subQuery);
				i++;
			}
			return this;
		}

		public Builder delete() {
			this.delete = true;
			return this;
		}

		public Builder delete(boolean delete) {
			if (delete) {
				this.delete = true;
			} else {
				this.delete = null;
			}
			return this;
		}

		public Query build() {
			Query query = new Query();
			if (this.startTime == null) {
				throw new IllegalArgumentException("the start time must be set");
			}
			if (this.startTime < MIN_START_TIME) {
				throw new IllegalArgumentException("the start time must be greater than " + MIN_START_TIME);
			}
			query.start = this.startTime;
			if (this.endTime != null) {
				if(this.endTime > MAX_END_TIME) {
					throw new IllegalArgumentException("the end time must be less than" + MAX_END_TIME);
				}
				if(this.endTime < this.startTime) {
					throw new IllegalArgumentException("the end time (" + this.endTime +
							") must be greater than start time (" + this.startTime + ")" );
				}
			}
			query.end = this.endTime;
			query.queries = this.subQueryList;
			query.delete = this.delete;
			query.msResolution = this.msResolution;
			return query;
		}

	}

	/**
	 * set the start time
	 * @param startTime start timestamp
	 * @return Builder get a builder
	 */
	public static Builder start(long startTime) {
		return new Builder(startTime);
	}

	/**
	 * set the start date
	 * @param startDate start date
	 * @return Builder
	 */
	public static Builder start(Date startDate) {
		long startTime = startDate.getTime();
		return new Builder(startTime);
	}

	/**
	 * set the start date and the end date
	 * @param startDate start date
	 * @param endDate end date
	 * @return Builder
	 */
	public static Builder timeRange(Date startDate, Date endDate) {
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		return new Builder(startTime, endTime);
	}

	/**
	 * set the start time and the end time
	 * @param startTime start timestamp
	 * @param endTime end timestamp
	 * @return Builder
	 */
	public static Builder timeRange(long startTime, long endTime) {
		return new Builder(startTime, endTime);
	}

	public Long getStart() {
		return start;
	}

	public Long getEnd() {
		return end;
	}

	public Boolean getDelete() {
		return delete;
	}

	public Boolean getMsResolution() {
		return msResolution;
	}

	public List<SubQuery> getQueries() {
		return queries;
	}

}
