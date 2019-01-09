package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.type.FilterType;

public class Filter {
    private FilterType type;
    private String tagk;
    private String filter;
    private Boolean groupBy;
    
    public static class Builder {
        private FilterType type;
        private String tagk;
        private String filter;
        private boolean groupBy;

        public Builder(FilterType type, String tagk, String filter, Boolean groupBy) {
            super();
            this.type = type;
            this.tagk = tagk;
            this.filter = filter;
            this.groupBy = groupBy;
        }

        public Builder(FilterType type, String tagk, String filter) {
            super();
            this.type = type;
            this.tagk = tagk;
            this.filter = filter;
        }
        
        public Filter build() {
            Filter f = new Filter();
            f.type = this.type;
            f.tagk = this.tagk;
            f.filter = this.filter;
            if(this.groupBy==true) {
                f.groupBy = this.groupBy;
            }
            
            return f;
        }
        
    }
    
    public static Builder filter(FilterType type,String tagk,String filter) {
        return new Builder(type,tagk,filter);
    }
    
    public static Builder filter(FilterType type,String tagk,String filter,Boolean groupBy) {
        return new Builder(type, tagk, filter, groupBy);
    }

    public static Builder filter(FilterType type,String filter) {
        return new Builder(type, null, filter);
    }

    public FilterType getType() {
        return type;
    }

    public void setType(FilterType type) {
        this.type = type;
    }

    public String getTagk() {
        return tagk;
    }

    public void setTagk(String tagk) {
        this.tagk = tagk;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public Boolean getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(Boolean groupBy) {
        this.groupBy = groupBy;
    }
}
