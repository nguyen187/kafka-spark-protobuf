package org.gtpvpair.common;

import java.io.Serializable;
import java.util.Objects;

public class CompositeKey implements Serializable {
    private final String sequenceNumber;
    private final String ip1;
    private final String ip2;

    public CompositeKey(String sequenceNumber, String ip1, String ip2) {
        this.sequenceNumber = sequenceNumber;
        // Đảm bảo ip1 luôn nhỏ hơn ip2 theo thứ tự từ điển
        if (ip1.compareTo(ip2) < 0) {
            this.ip1 = ip1;
            this.ip2 = ip2;
        } else {
            this.ip1 = ip2;
            this.ip2 = ip1;
        }
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public String getIp1() {
        return ip1;
    }

    public String getIp2() {
        return ip2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Objects.equals(sequenceNumber, that.sequenceNumber) &&
                Objects.equals(ip1, that.ip1) &&
                Objects.equals(ip2, that.ip2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequenceNumber, ip1, ip2);
    }

    @Override
    public String toString() {
        return "CompositeKey{" +
                "sequenceNumber='" + sequenceNumber + '\'' +
                ", ip1='" + ip1 + '\'' +
                ", ip2='" + ip2 + '\'' +
                '}';
    }
}
