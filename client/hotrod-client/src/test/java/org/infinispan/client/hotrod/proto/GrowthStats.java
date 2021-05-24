package org.infinispan.client.hotrod.proto;

import java.util.List;
import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoField;


public class GrowthStats {
    public String status;
    public String postcode;
    public String postcode_type;
    public Long effective_date;
    public String url;
    public List<List<String>> data;
    public String process_time;

    @ProtoField(number = 1)
    public String getStatus() {
        return status;
    }

    @ProtoField(number = 2)
    public String getPostcode() {
        return postcode;
    }

    @ProtoField(number = 3)
    public String getPostcode_type() {
        return postcode_type;
    }

    @ProtoField(number = 4)
    public Long getEffective_date() {
        return effective_date;
    }

    @ProtoField(number = 5)
    public String getUrl() {
        return url;
    }

    public List<List<String>> getData() { return data; }

    @ProtoField(number = 6)
    ListOfWrappedLists<String> getProtoData() {
        return new ListOfWrappedLists<>(data);
    }

    void setProtoData(ListOfWrappedLists<String> protoData) {
       data = protoData.getLists();
    }

    @ProtoField(number = 7)
    public String getProcess_time() {
        return process_time;
    }

   public void setStatus(String status) {
      this.status = status;
   }

   public void setPostcode(String postcode) {
      this.postcode = postcode;
   }

   public void setPostcode_type(String postcode_type) {
      this.postcode_type = postcode_type;
   }

   public void setEffective_date(Long effective_date) {
      this.effective_date = effective_date;
   }

   public void setUrl(String url) {
      this.url = url;
   }

   public void setData(List<List<String>> data) {
      this.data = data;
   }

   public void setProcess_time(String process_time) {
      this.process_time = process_time;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GrowthStats that = (GrowthStats) o;
      return Objects.equals(status, that.status) && Objects.equals(postcode, that.postcode) &&
             Objects.equals(postcode_type, that.postcode_type) && Objects.equals(effective_date, that.effective_date) &&
             Objects.equals(url, that.url) && Objects.equals(data, that.data) &&
             Objects.equals(process_time, that.process_time);
   }

   @Override
   public int hashCode() {
      return Objects.hash(status, postcode, postcode_type, effective_date, url, data, process_time);
   }

   @Override
   public String toString() {
      return "GrowthStats{" +
             "status='" + status + '\'' +
             ", postcode='" + postcode + '\'' +
             ", postcode_type='" + postcode_type + '\'' +
             ", effective_date=" + effective_date +
             ", url='" + url + '\'' +
             ", data=" + data +
             ", process_time='" + process_time + '\'' +
             '}';
   }
}
