package com.aliyun.hitsdb.client.value.request;

/**
 * Created By jianhong.hjh
 * Date: 2018/11/5
 */
public class RateOptions {

    private Boolean counter;

    private Boolean dropResets;

    private Long counterMax;

    private Long resetValue;

    public static class Builder {

        private Boolean counter;

        private Boolean dropResets;

        private Long counterMax;

        private Long resetValue;

        public Builder() {

        }

        public Builder counter(boolean counter) {
            this.counter = counter;
            return this;
        }

        public Builder dropResets(boolean dropResets) {
            this.dropResets = dropResets;
            return this;
        }

        public Builder counterMax(long counterMax) {
            this.counterMax = counterMax;
            return this;
        }

        public Builder resetValue(long resetValue) {
            this.resetValue = resetValue;
            return this;
        }

        public RateOptions build() {
            RateOptions rateOptions = new RateOptions();
            if (counter != null) {
                rateOptions.counter = counter;
            }
            if (dropResets != null) {
                rateOptions.dropResets = dropResets;
            }
            if (counterMax != null) {
                rateOptions.counterMax = counterMax;
            }
            if (resetValue != null) {
                rateOptions.resetValue = resetValue;
            }
            return rateOptions;
        }
    }


    public static Builder newBuilder() {
        return new Builder();
    }

    public Boolean getCounter() {
        return counter;
    }

    public void setCounter(Boolean counter) {
        this.counter = counter;
    }

    public Boolean getDropResets() {
        return dropResets;
    }

    public void setDropResets(Boolean dropResets) {
        this.dropResets = dropResets;
    }

    public Long getCounterMax() {
        return counterMax;
    }

    public void setCounterMax(Long counterMax) {
        this.counterMax = counterMax;
    }

    public Long getResetValue() {
        return resetValue;
    }

    public void setResetValue(Long resetValue) {
        this.resetValue = resetValue;
    }
}
