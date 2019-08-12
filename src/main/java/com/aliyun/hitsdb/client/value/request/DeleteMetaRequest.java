package com.aliyun.hitsdb.client.value.request;

import java.util.List;
import java.util.Map;

public class DeleteMetaRequest extends Timeline {
    public static class Builder extends Timeline.Builder {
        // the data is deleted at the same time by default
        private boolean deleteData = true;
        private boolean recursive;

        public Builder(String metric) {
            super(metric);
            this.deleteData = true;   // the data is deleted at the same time by default
            this.recursive = true;    // the timelines are deleted recursively by default
        }

        public Builder tag(String tagk, String tagv) {
            super.tag(tagk, tagv);
            return this;
        }

        public Builder tag(Map<String, String> tags) {
            super.tag(tags);
            return this;
        }

        public Builder fields(List<String> fields) {
            super.fields(fields);
            return this;
        }

        public Builder deleteData(boolean deleteData) {
            this.deleteData = deleteData;
            return this;
        }

        public Builder recursive(boolean recursive) {
            this.recursive = recursive;
            return this;
        }

        public DeleteMetaRequest build() {
            DeleteMetaRequest request = new DeleteMetaRequest();

            Timeline timeline = super.build();
            request.setMetric(timeline.getMetric());
            request.setTags(timeline.getTags());
            if ((timeline.getFields() != null) && (!timeline.getFields().isEmpty())) {
                request.setFields(timeline.getFields());
            }
            request.setDeleteData(this.deleteData);
            request.setRecursive(this.recursive);

            return request;
        }
    }

    private boolean deleteData;
    private boolean recursive;

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public boolean isDeleteData() {
        return deleteData;
    }

    public void setDeleteData(boolean deleteData) {
        this.deleteData = deleteData;
    }

    public DeleteMetaRequest() {
        super();
        this.deleteData = true;
        this.recursive = true;
    }

    public static DeleteMetaRequest.Builder metric(String metric) {
        return new DeleteMetaRequest.Builder(metric);
    }
}
