package com.creek.storm.target.fates;

import java.util.List;

public class FatesListElement<T> {
    private long memberSeq = -1L;
    private String subject = null;
    private String field = null;
    private List<T> list = null;//value的可能形式
    private boolean useOldValue = true;//为true表示用到fates中原有数据      
    
    public FatesListElement(long memberSeq, String subject, String field, List<T> list,
                        boolean useOldValue) {
        super();
        this.memberSeq = memberSeq;
        this.subject = subject;
        this.field = field;
        this.list = list;
        this.useOldValue = useOldValue;
    }
    
    @Override
    public String toString() {
        return "FatesElement [memberSeq=" + memberSeq + ", subject=" + subject + ", field=" + field
                + ", list=" + list + ", useOldValue=" + useOldValue + "]";
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((field == null) ? 0 : field.hashCode());
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        result = prime * result + (int) (memberSeq ^ (memberSeq >>> 32));
        result = prime * result + ((subject == null) ? 0 : subject.hashCode());
        result = prime * result + (useOldValue ? 1231 : 1237);
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
        @SuppressWarnings("rawtypes")
        FatesListElement other = (FatesListElement) obj;
        if (field == null) {
            if (other.field != null)
                return false;
        } else if (!field.equals(other.field))
            return false;
        if (list == null) {
            if (other.list != null)
                return false;
        } else if (!list.equals(other.list))
            return false;
        if (memberSeq != other.memberSeq)
            return false;
        if (subject == null) {
            if (other.subject != null)
                return false;
        } else if (!subject.equals(other.subject))
            return false;
        if (useOldValue != other.useOldValue)
            return false;
        return true;
    }

    public long getMemberSeq() {
        return memberSeq;
    }
    
    public void setMemberSeq(long memberSeq) {
        this.memberSeq = memberSeq;
    }
    
    public String getSubject() {
        return subject;
    }
    
    public void setSubject(String subject) {
        this.subject = subject;
    }
    
    public String getField() {
        return field;
    }
    
    public void setField(String field) {
        this.field = field;
    }
    
    public List<T> getList() {
        return list;
    }
    
    public void setList(List<T> list) {
        this.list = list;
    }
    
    public boolean isUseOldValue() {
        return useOldValue;
    }

    public void setUseOldValue(boolean useOldValue) {
        this.useOldValue = useOldValue;
    }
}
