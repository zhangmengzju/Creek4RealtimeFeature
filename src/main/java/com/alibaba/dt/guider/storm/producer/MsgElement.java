package com.alibaba.dt.guider.storm.producer;

public class MsgElement {

    private long memberSeq = -1L;
    private String itemId  = null;
    private int countDelta = -1;//-1表示为metaq delay msg，收到这种消息不去更新Latest100Item
    private long timestamp = -1L;
    private long cateId = -1L;//叶子类目Id
    
    public MsgElement(long memberSeq, String itemId, int countDelta, long timestamp, long cateId) {
        super();
        this.memberSeq = memberSeq;
        this.itemId = itemId;
        this.countDelta = countDelta;
        this.timestamp = timestamp;
        this.cateId = cateId;
    }

    @Override
    public String toString() {
        return "QueueElement [memberSeq=" + memberSeq + ", itemId=" + itemId + ", countDelta="
                + countDelta + ", timestamp=" + timestamp + ", cateId=" + cateId + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (cateId ^ (cateId >>> 32));
        result = prime * result + countDelta;
        result = prime * result + ((itemId == null) ? 0 : itemId.hashCode());
        result = prime * result + (int) (memberSeq ^ (memberSeq >>> 32));
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MsgElement other = (MsgElement) obj;
        if (cateId != other.cateId)
            return false;
        if (countDelta != other.countDelta)
            return false;
        if (itemId == null) {
            if (other.itemId != null)
                return false;
        } else if (!itemId.equals(other.itemId))
            return false;
        if (memberSeq != other.memberSeq)
            return false;
        if (timestamp != other.timestamp)
            return false;
        return true;
    }

    public long getMemberSeq() {
        return memberSeq;
    }

    public void setMemberSeq(long memberSeq) {
        this.memberSeq = memberSeq;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public int getCountDelta() {
        return countDelta;
    }

    public void setCountDelta(int countDelta) {
        this.countDelta = countDelta;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getCateId() {
        return cateId;
    }

    public void setCateId(long cateId) {
        this.cateId = cateId;
    }
}
