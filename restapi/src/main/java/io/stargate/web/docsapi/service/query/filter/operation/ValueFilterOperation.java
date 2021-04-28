package io.stargate.web.docsapi.service.query.filter.operation;

public interface ValueFilterOperation extends BaseFilterOperation {

  /**
   * Tests the filter value and database against this predicate.
   *
   * @param filterValue Filter value, can be <code>null</code>
   * @param dbValue DB value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the predicate, otherwise <code>false
   *     </code>
   */
  boolean test(String filterValue, String dbValue);

  /**
   * Validates if the filter input can be used against this predicate.
   *
   * <p>Implementations should throw proper exception in case the validation fails.
   *
   * @param filterValue Filter value, can be <code>null</code>
   */
  default void validateStringFilterInput(String filterValue) {
    // default impl empty
  };

  /**
   * Tests the filter value and database against this predicate.
   *
   * @param filterValue Filter value, can be <code>null</code>
   * @param dbValue DB value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the predicate, otherwise <code>false
   *     </code>
   */
  boolean test(Number filterValue, Double dbValue);

  /**
   * Validates if the filter input can be used against this predicate.
   *
   * <p>Implementations should throw proper exception in case the validation fails.
   *
   * @param filterValue Filter value, can be <code>null</code>
   */
  default void validateDoubleFilterInput(Number filterValue) {
    // default impl empty
  };

  /**
   * Tests the filter value and database against this predicate.
   *
   * @param filterValue Filter value, can be <code>null</code>
   * @param dbValue DB value, can be <code>null</code>
   * @return <code>true</code> if the filter value matches the predicate, otherwise <code>false
   *     </code>
   */
  boolean test(Boolean filterValue, Boolean dbValue);

  /**
   * Validates if the filter input can be used against this predicate.
   *
   * <p>Implementations should throw proper exception in case the validation fails.
   *
   * @param filterValue Filter value, can be <code>null</code>
   */
  default void validateBooleanFilterInput(Boolean filterValue) {
    // default impl empty
  };
}
