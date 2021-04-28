package io.stargate.web.docsapi.service.query.filter.operation;

import io.stargate.web.docsapi.service.query.condition.provider.ConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.BasicConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.ExistsConditionProvider;
import io.stargate.web.docsapi.service.query.condition.provider.impl.ListConditionProvider;
import io.stargate.web.docsapi.service.query.filter.operation.impl.*;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Maps raw filter values to the condition providers with specific filer operation.
 */
public enum FilterOperationCode {

    // all existing operations
    EQ("$eq", BasicConditionProvider.of(EqFilterOperation.of())),
    NE("$ne", BasicConditionProvider.of(NeFilterOperation.of())),
    LT("$lt", BasicConditionProvider.of(LtFilterOperation.of())),
    LTE("$lte", BasicConditionProvider.of(LteFilterOperation.of())),
    GT("$gt", BasicConditionProvider.of(GtFilterOperation.of())),
    GTE("$gte", BasicConditionProvider.of(GteFilterOperation.of())),
    EXISTS("$exists", new ExistsConditionProvider()),
    IN("$in", ListConditionProvider.of(InFilterOperation.of())),
    NIN("$nin", ListConditionProvider.of(NotInFilterOperation.of()))
    ;

    /**
     * Raw value.
     */
    private final String rawValue;

    /**
     * Condition provider.
     */
    private final ConditionProvider conditionProvider;

    FilterOperationCode(String rawValue, ConditionProvider conditionProvider) {
        this.rawValue = rawValue;
        this.conditionProvider = conditionProvider;
    }

    public String getRawValue() {
        return rawValue;
    }

    public ConditionProvider getConditionProvider() {
        return conditionProvider;
    }

    /**
     * Finds a {@link FilterOperationCode} by raw value.
     *
     * @param rawValue
     * @return
     */
    public static Optional<FilterOperationCode> getByRawValue(String rawValue) {
        return Arrays.stream(FilterOperationCode.values()).filter(op -> Objects.equals(op.rawValue, rawValue)).findFirst();
    }

}
