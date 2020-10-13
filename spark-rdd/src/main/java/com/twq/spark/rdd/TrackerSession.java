/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.twq.spark.rdd;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class TrackerSession extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TrackerSession\",\"namespace\":\"com.twq.spark.rdd\",\"fields\":[{\"name\":\"session_id\",\"type\":\"string\"},{\"name\":\"session_server_time\",\"type\":\"string\"},{\"name\":\"cookie\",\"type\":\"string\"},{\"name\":\"cookie_label\",\"type\":\"string\"},{\"name\":\"ip\",\"type\":\"string\"},{\"name\":\"landing_url\",\"type\":\"string\"},{\"name\":\"pageview_count\",\"type\":\"int\"},{\"name\":\"click_count\",\"type\":\"int\"},{\"name\":\"domain\",\"type\":\"string\"},{\"name\":\"domain_label\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence session_id;
  @Deprecated public java.lang.CharSequence session_server_time;
  @Deprecated public java.lang.CharSequence cookie;
  @Deprecated public java.lang.CharSequence cookie_label;
  @Deprecated public java.lang.CharSequence ip;
  @Deprecated public java.lang.CharSequence landing_url;
  @Deprecated public int pageview_count;
  @Deprecated public int click_count;
  @Deprecated public java.lang.CharSequence domain;
  @Deprecated public java.lang.CharSequence domain_label;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public TrackerSession() {}

  /**
   * All-args constructor.
   */
  public TrackerSession(java.lang.CharSequence session_id, java.lang.CharSequence session_server_time, java.lang.CharSequence cookie, java.lang.CharSequence cookie_label, java.lang.CharSequence ip, java.lang.CharSequence landing_url, java.lang.Integer pageview_count, java.lang.Integer click_count, java.lang.CharSequence domain, java.lang.CharSequence domain_label) {
    this.session_id = session_id;
    this.session_server_time = session_server_time;
    this.cookie = cookie;
    this.cookie_label = cookie_label;
    this.ip = ip;
    this.landing_url = landing_url;
    this.pageview_count = pageview_count;
    this.click_count = click_count;
    this.domain = domain;
    this.domain_label = domain_label;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return session_id;
    case 1: return session_server_time;
    case 2: return cookie;
    case 3: return cookie_label;
    case 4: return ip;
    case 5: return landing_url;
    case 6: return pageview_count;
    case 7: return click_count;
    case 8: return domain;
    case 9: return domain_label;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: session_id = (java.lang.CharSequence)value$; break;
    case 1: session_server_time = (java.lang.CharSequence)value$; break;
    case 2: cookie = (java.lang.CharSequence)value$; break;
    case 3: cookie_label = (java.lang.CharSequence)value$; break;
    case 4: ip = (java.lang.CharSequence)value$; break;
    case 5: landing_url = (java.lang.CharSequence)value$; break;
    case 6: pageview_count = (java.lang.Integer)value$; break;
    case 7: click_count = (java.lang.Integer)value$; break;
    case 8: domain = (java.lang.CharSequence)value$; break;
    case 9: domain_label = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'session_id' field.
   */
  public java.lang.CharSequence getSessionId() {
    return session_id;
  }

  /**
   * Sets the value of the 'session_id' field.
   * @param value the value to set.
   */
  public void setSessionId(java.lang.CharSequence value) {
    this.session_id = value;
  }

  /**
   * Gets the value of the 'session_server_time' field.
   */
  public java.lang.CharSequence getSessionServerTime() {
    return session_server_time;
  }

  /**
   * Sets the value of the 'session_server_time' field.
   * @param value the value to set.
   */
  public void setSessionServerTime(java.lang.CharSequence value) {
    this.session_server_time = value;
  }

  /**
   * Gets the value of the 'cookie' field.
   */
  public java.lang.CharSequence getCookie() {
    return cookie;
  }

  /**
   * Sets the value of the 'cookie' field.
   * @param value the value to set.
   */
  public void setCookie(java.lang.CharSequence value) {
    this.cookie = value;
  }

  /**
   * Gets the value of the 'cookie_label' field.
   */
  public java.lang.CharSequence getCookieLabel() {
    return cookie_label;
  }

  /**
   * Sets the value of the 'cookie_label' field.
   * @param value the value to set.
   */
  public void setCookieLabel(java.lang.CharSequence value) {
    this.cookie_label = value;
  }

  /**
   * Gets the value of the 'ip' field.
   */
  public java.lang.CharSequence getIp() {
    return ip;
  }

  /**
   * Sets the value of the 'ip' field.
   * @param value the value to set.
   */
  public void setIp(java.lang.CharSequence value) {
    this.ip = value;
  }

  /**
   * Gets the value of the 'landing_url' field.
   */
  public java.lang.CharSequence getLandingUrl() {
    return landing_url;
  }

  /**
   * Sets the value of the 'landing_url' field.
   * @param value the value to set.
   */
  public void setLandingUrl(java.lang.CharSequence value) {
    this.landing_url = value;
  }

  /**
   * Gets the value of the 'pageview_count' field.
   */
  public java.lang.Integer getPageviewCount() {
    return pageview_count;
  }

  /**
   * Sets the value of the 'pageview_count' field.
   * @param value the value to set.
   */
  public void setPageviewCount(java.lang.Integer value) {
    this.pageview_count = value;
  }

  /**
   * Gets the value of the 'click_count' field.
   */
  public java.lang.Integer getClickCount() {
    return click_count;
  }

  /**
   * Sets the value of the 'click_count' field.
   * @param value the value to set.
   */
  public void setClickCount(java.lang.Integer value) {
    this.click_count = value;
  }

  /**
   * Gets the value of the 'domain' field.
   */
  public java.lang.CharSequence getDomain() {
    return domain;
  }

  /**
   * Sets the value of the 'domain' field.
   * @param value the value to set.
   */
  public void setDomain(java.lang.CharSequence value) {
    this.domain = value;
  }

  /**
   * Gets the value of the 'domain_label' field.
   */
  public java.lang.CharSequence getDomainLabel() {
    return domain_label;
  }

  /**
   * Sets the value of the 'domain_label' field.
   * @param value the value to set.
   */
  public void setDomainLabel(java.lang.CharSequence value) {
    this.domain_label = value;
  }

  /** Creates a new TrackerSession RecordBuilder */
  public static com.twq.spark.rdd.TrackerSession.Builder newBuilder() {
    return new com.twq.spark.rdd.TrackerSession.Builder();
  }
  
  /** Creates a new TrackerSession RecordBuilder by copying an existing Builder */
  public static com.twq.spark.rdd.TrackerSession.Builder newBuilder(com.twq.spark.rdd.TrackerSession.Builder other) {
    return new com.twq.spark.rdd.TrackerSession.Builder(other);
  }
  
  /** Creates a new TrackerSession RecordBuilder by copying an existing TrackerSession instance */
  public static com.twq.spark.rdd.TrackerSession.Builder newBuilder(com.twq.spark.rdd.TrackerSession other) {
    return new com.twq.spark.rdd.TrackerSession.Builder(other);
  }
  
  /**
   * RecordBuilder for TrackerSession instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TrackerSession>
    implements org.apache.avro.data.RecordBuilder<TrackerSession> {

    private java.lang.CharSequence session_id;
    private java.lang.CharSequence session_server_time;
    private java.lang.CharSequence cookie;
    private java.lang.CharSequence cookie_label;
    private java.lang.CharSequence ip;
    private java.lang.CharSequence landing_url;
    private int pageview_count;
    private int click_count;
    private java.lang.CharSequence domain;
    private java.lang.CharSequence domain_label;

    /** Creates a new Builder */
    private Builder() {
      super(com.twq.spark.rdd.TrackerSession.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.twq.spark.rdd.TrackerSession.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.session_id)) {
        this.session_id = data().deepCopy(fields()[0].schema(), other.session_id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.session_server_time)) {
        this.session_server_time = data().deepCopy(fields()[1].schema(), other.session_server_time);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.cookie)) {
        this.cookie = data().deepCopy(fields()[2].schema(), other.cookie);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.cookie_label)) {
        this.cookie_label = data().deepCopy(fields()[3].schema(), other.cookie_label);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.ip)) {
        this.ip = data().deepCopy(fields()[4].schema(), other.ip);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.landing_url)) {
        this.landing_url = data().deepCopy(fields()[5].schema(), other.landing_url);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.pageview_count)) {
        this.pageview_count = data().deepCopy(fields()[6].schema(), other.pageview_count);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.click_count)) {
        this.click_count = data().deepCopy(fields()[7].schema(), other.click_count);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.domain)) {
        this.domain = data().deepCopy(fields()[8].schema(), other.domain);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.domain_label)) {
        this.domain_label = data().deepCopy(fields()[9].schema(), other.domain_label);
        fieldSetFlags()[9] = true;
      }
    }
    
    /** Creates a Builder by copying an existing TrackerSession instance */
    private Builder(com.twq.spark.rdd.TrackerSession other) {
            super(com.twq.spark.rdd.TrackerSession.SCHEMA$);
      if (isValidValue(fields()[0], other.session_id)) {
        this.session_id = data().deepCopy(fields()[0].schema(), other.session_id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.session_server_time)) {
        this.session_server_time = data().deepCopy(fields()[1].schema(), other.session_server_time);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.cookie)) {
        this.cookie = data().deepCopy(fields()[2].schema(), other.cookie);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.cookie_label)) {
        this.cookie_label = data().deepCopy(fields()[3].schema(), other.cookie_label);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.ip)) {
        this.ip = data().deepCopy(fields()[4].schema(), other.ip);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.landing_url)) {
        this.landing_url = data().deepCopy(fields()[5].schema(), other.landing_url);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.pageview_count)) {
        this.pageview_count = data().deepCopy(fields()[6].schema(), other.pageview_count);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.click_count)) {
        this.click_count = data().deepCopy(fields()[7].schema(), other.click_count);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.domain)) {
        this.domain = data().deepCopy(fields()[8].schema(), other.domain);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.domain_label)) {
        this.domain_label = data().deepCopy(fields()[9].schema(), other.domain_label);
        fieldSetFlags()[9] = true;
      }
    }

    /** Gets the value of the 'session_id' field */
    public java.lang.CharSequence getSessionId() {
      return session_id;
    }
    
    /** Sets the value of the 'session_id' field */
    public com.twq.spark.rdd.TrackerSession.Builder setSessionId(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.session_id = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'session_id' field has been set */
    public boolean hasSessionId() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'session_id' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearSessionId() {
      session_id = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'session_server_time' field */
    public java.lang.CharSequence getSessionServerTime() {
      return session_server_time;
    }
    
    /** Sets the value of the 'session_server_time' field */
    public com.twq.spark.rdd.TrackerSession.Builder setSessionServerTime(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.session_server_time = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'session_server_time' field has been set */
    public boolean hasSessionServerTime() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'session_server_time' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearSessionServerTime() {
      session_server_time = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'cookie' field */
    public java.lang.CharSequence getCookie() {
      return cookie;
    }
    
    /** Sets the value of the 'cookie' field */
    public com.twq.spark.rdd.TrackerSession.Builder setCookie(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.cookie = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'cookie' field has been set */
    public boolean hasCookie() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'cookie' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearCookie() {
      cookie = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'cookie_label' field */
    public java.lang.CharSequence getCookieLabel() {
      return cookie_label;
    }
    
    /** Sets the value of the 'cookie_label' field */
    public com.twq.spark.rdd.TrackerSession.Builder setCookieLabel(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.cookie_label = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'cookie_label' field has been set */
    public boolean hasCookieLabel() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'cookie_label' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearCookieLabel() {
      cookie_label = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'ip' field */
    public java.lang.CharSequence getIp() {
      return ip;
    }
    
    /** Sets the value of the 'ip' field */
    public com.twq.spark.rdd.TrackerSession.Builder setIp(java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.ip = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'ip' field has been set */
    public boolean hasIp() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'ip' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearIp() {
      ip = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /** Gets the value of the 'landing_url' field */
    public java.lang.CharSequence getLandingUrl() {
      return landing_url;
    }
    
    /** Sets the value of the 'landing_url' field */
    public com.twq.spark.rdd.TrackerSession.Builder setLandingUrl(java.lang.CharSequence value) {
      validate(fields()[5], value);
      this.landing_url = value;
      fieldSetFlags()[5] = true;
      return this; 
    }
    
    /** Checks whether the 'landing_url' field has been set */
    public boolean hasLandingUrl() {
      return fieldSetFlags()[5];
    }
    
    /** Clears the value of the 'landing_url' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearLandingUrl() {
      landing_url = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /** Gets the value of the 'pageview_count' field */
    public java.lang.Integer getPageviewCount() {
      return pageview_count;
    }
    
    /** Sets the value of the 'pageview_count' field */
    public com.twq.spark.rdd.TrackerSession.Builder setPageviewCount(int value) {
      validate(fields()[6], value);
      this.pageview_count = value;
      fieldSetFlags()[6] = true;
      return this; 
    }
    
    /** Checks whether the 'pageview_count' field has been set */
    public boolean hasPageviewCount() {
      return fieldSetFlags()[6];
    }
    
    /** Clears the value of the 'pageview_count' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearPageviewCount() {
      fieldSetFlags()[6] = false;
      return this;
    }

    /** Gets the value of the 'click_count' field */
    public java.lang.Integer getClickCount() {
      return click_count;
    }
    
    /** Sets the value of the 'click_count' field */
    public com.twq.spark.rdd.TrackerSession.Builder setClickCount(int value) {
      validate(fields()[7], value);
      this.click_count = value;
      fieldSetFlags()[7] = true;
      return this; 
    }
    
    /** Checks whether the 'click_count' field has been set */
    public boolean hasClickCount() {
      return fieldSetFlags()[7];
    }
    
    /** Clears the value of the 'click_count' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearClickCount() {
      fieldSetFlags()[7] = false;
      return this;
    }

    /** Gets the value of the 'domain' field */
    public java.lang.CharSequence getDomain() {
      return domain;
    }
    
    /** Sets the value of the 'domain' field */
    public com.twq.spark.rdd.TrackerSession.Builder setDomain(java.lang.CharSequence value) {
      validate(fields()[8], value);
      this.domain = value;
      fieldSetFlags()[8] = true;
      return this; 
    }
    
    /** Checks whether the 'domain' field has been set */
    public boolean hasDomain() {
      return fieldSetFlags()[8];
    }
    
    /** Clears the value of the 'domain' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearDomain() {
      domain = null;
      fieldSetFlags()[8] = false;
      return this;
    }

    /** Gets the value of the 'domain_label' field */
    public java.lang.CharSequence getDomainLabel() {
      return domain_label;
    }
    
    /** Sets the value of the 'domain_label' field */
    public com.twq.spark.rdd.TrackerSession.Builder setDomainLabel(java.lang.CharSequence value) {
      validate(fields()[9], value);
      this.domain_label = value;
      fieldSetFlags()[9] = true;
      return this; 
    }
    
    /** Checks whether the 'domain_label' field has been set */
    public boolean hasDomainLabel() {
      return fieldSetFlags()[9];
    }
    
    /** Clears the value of the 'domain_label' field */
    public com.twq.spark.rdd.TrackerSession.Builder clearDomainLabel() {
      domain_label = null;
      fieldSetFlags()[9] = false;
      return this;
    }

    @Override
    public TrackerSession build() {
      try {
        TrackerSession record = new TrackerSession();
        record.session_id = fieldSetFlags()[0] ? this.session_id : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.session_server_time = fieldSetFlags()[1] ? this.session_server_time : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.cookie = fieldSetFlags()[2] ? this.cookie : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.cookie_label = fieldSetFlags()[3] ? this.cookie_label : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.ip = fieldSetFlags()[4] ? this.ip : (java.lang.CharSequence) defaultValue(fields()[4]);
        record.landing_url = fieldSetFlags()[5] ? this.landing_url : (java.lang.CharSequence) defaultValue(fields()[5]);
        record.pageview_count = fieldSetFlags()[6] ? this.pageview_count : (java.lang.Integer) defaultValue(fields()[6]);
        record.click_count = fieldSetFlags()[7] ? this.click_count : (java.lang.Integer) defaultValue(fields()[7]);
        record.domain = fieldSetFlags()[8] ? this.domain : (java.lang.CharSequence) defaultValue(fields()[8]);
        record.domain_label = fieldSetFlags()[9] ? this.domain_label : (java.lang.CharSequence) defaultValue(fields()[9]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
