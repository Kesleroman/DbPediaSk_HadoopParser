/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class DbPage extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"DbPage\",\"namespace\":\"avro\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"int\"]},{\"name\":\"pageLabel\",\"type\":[\"null\",\"string\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.Integer id;
  @Deprecated public java.lang.CharSequence pageLabel;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public DbPage() {}

  /**
   * All-args constructor.
   */
  public DbPage(java.lang.Integer id, java.lang.CharSequence pageLabel) {
    this.id = id;
    this.pageLabel = pageLabel;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return pageLabel;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Integer)value$; break;
    case 1: pageLabel = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   */
  public java.lang.Integer getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(java.lang.Integer value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'pageLabel' field.
   */
  public java.lang.CharSequence getPageLabel() {
    return pageLabel;
  }

  /**
   * Sets the value of the 'pageLabel' field.
   * @param value the value to set.
   */
  public void setPageLabel(java.lang.CharSequence value) {
    this.pageLabel = value;
  }

  /** Creates a new DbPage RecordBuilder */
  public static avro.DbPage.Builder newBuilder() {
    return new avro.DbPage.Builder();
  }
  
  /** Creates a new DbPage RecordBuilder by copying an existing Builder */
  public static avro.DbPage.Builder newBuilder(avro.DbPage.Builder other) {
    return new avro.DbPage.Builder(other);
  }
  
  /** Creates a new DbPage RecordBuilder by copying an existing DbPage instance */
  public static avro.DbPage.Builder newBuilder(avro.DbPage other) {
    return new avro.DbPage.Builder(other);
  }
  
  /**
   * RecordBuilder for DbPage instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<DbPage>
    implements org.apache.avro.data.RecordBuilder<DbPage> {

    private java.lang.Integer id;
    private java.lang.CharSequence pageLabel;

    /** Creates a new Builder */
    private Builder() {
      super(avro.DbPage.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(avro.DbPage.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.pageLabel)) {
        this.pageLabel = data().deepCopy(fields()[1].schema(), other.pageLabel);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing DbPage instance */
    private Builder(avro.DbPage other) {
            super(avro.DbPage.SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.pageLabel)) {
        this.pageLabel = data().deepCopy(fields()[1].schema(), other.pageLabel);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'id' field */
    public java.lang.Integer getId() {
      return id;
    }
    
    /** Sets the value of the 'id' field */
    public avro.DbPage.Builder setId(java.lang.Integer value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'id' field has been set */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'id' field */
    public avro.DbPage.Builder clearId() {
      id = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'pageLabel' field */
    public java.lang.CharSequence getPageLabel() {
      return pageLabel;
    }
    
    /** Sets the value of the 'pageLabel' field */
    public avro.DbPage.Builder setPageLabel(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.pageLabel = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'pageLabel' field has been set */
    public boolean hasPageLabel() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'pageLabel' field */
    public avro.DbPage.Builder clearPageLabel() {
      pageLabel = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public DbPage build() {
      try {
        DbPage record = new DbPage();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.Integer) defaultValue(fields()[0]);
        record.pageLabel = fieldSetFlags()[1] ? this.pageLabel : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
