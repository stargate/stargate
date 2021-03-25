package io.stargate.web.models;

import java.io.Serializable;
import java.util.List;
import javax.validation.constraints.NotNull;

/**
 * Pojo to be used at API level to create a new UserDefinedType.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class UserDefinedTypeAdd implements Serializable {

  /** Serial. */
  private static final long serialVersionUID = -3119628723445448877L;

  @NotNull private String name;

  private boolean ifNotExists = false;

  @NotNull private List<ColumnDefinitionUserDefinedType> columnDefinitions;

  /**
   * Getter accessor for attribute 'name'.
   *
   * @return current value of 'name'
   */
  public String getName() {
    return name;
  }

  /**
   * Setter accessor for attribute 'name'.
   *
   * @param name new value for 'name '
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Getter accessor for attribute 'ifNotExists'.
   *
   * @return current value of 'ifNotExists'
   */
  public boolean isIfNotExists() {
    return ifNotExists;
  }

  /**
   * Setter accessor for attribute 'ifNotExists'.
   *
   * @param ifNotExists new value for 'ifNotExists '
   */
  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  /**
   * Getter accessor for attribute 'columnDefinitions'.
   *
   * @return current value of 'columnDefinitions'
   */
  public List<ColumnDefinitionUserDefinedType> getColumnDefinitions() {
    return columnDefinitions;
  }

  /**
   * Setter accessor for attribute 'columnDefinitions'.
   *
   * @param columnDefinitions new value for 'columnDefinitions '
   */
  public void setColumnDefinitions(List<ColumnDefinitionUserDefinedType> columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }
}
