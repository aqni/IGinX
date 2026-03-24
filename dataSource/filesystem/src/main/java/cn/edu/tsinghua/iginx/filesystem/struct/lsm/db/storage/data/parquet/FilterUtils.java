package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.*;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class FilterUtils {

    private FilterUtils() {
    }

    public static Predicate toPaimonPredicate(Filter filter, Header header, RowType projectedSchema) {
        switch (filter.getType()) {
            case Key:
                return toPaimonPredicate((KeyFilter) filter, projectedSchema);
            case Value:
                return toPaimonPredicate((ValueFilter) filter, header, projectedSchema);
            case And:
                return toPaimonPredicate((AndFilter) filter, header, projectedSchema);
            case Or:
                return toPaimonPredicate((OrFilter) filter, header, projectedSchema);
            case In:
                return toPaimonPredicate((InFilter) filter, header, projectedSchema);
            case Path:
            case Bool:
            case Expr:
            case Not:
            default:
                throw new UnsupportedOperationException("Unsupported filter type: " + filter.getType());
        }
    }

    private static Predicate toPaimonPredicate(KeyFilter filter, RowType projectedSchema) {
        PredicateBuilder builder = new PredicateBuilder(projectedSchema);
        switch (filter.getOp()) {
            case GE:
            case GE_AND:
                return builder.greaterOrEqual(0, filter.getValue());
            case G:
            case G_AND:
                return builder.greaterThan(0, filter.getValue());
            case LE:
            case LE_AND:
                return builder.lessOrEqual(0, filter.getValue());
            case L:
            case L_AND:
                return builder.lessThan(0, filter.getValue());
            case E:
            case E_AND:
                return builder.equal(0, filter.getValue());
            case NE:
            case NE_AND:
                return builder.notEqual(0, filter.getValue());
            case LIKE:
            case LIKE_AND:
            case NOT_LIKE:
            case NOT_LIKE_AND:
            default:
                throw new UnsupportedOperationException("Unsupported filter operator: " + filter.getOp());
        }
    }

    private static Predicate toPaimonPredicate(InFilter filter, Header header, RowType projectedSchema) {
        PredicateBuilder builder = new PredicateBuilder(projectedSchema);

        int index = getIndex(filter.getPath(), header);
        List<Object> values = new ArrayList<>();
        for (Value value : filter.getValues()) {
            values.add(getJavaObject(header, index, value));
        }
        DataField field = projectedSchema.getFields().get(index);
        switch (filter.getInOp()) {
            case IN_AND:
            case IN_OR:
                if(values.size() == 1) {
                    return builder.equal(index, values.get(0));
                }
                return new LeafPredicate(In.INSTANCE, field.type(), index, field.name(), values);
            case NOT_IN_AND:
            case NOT_IN_OR:
                if(values.size() == 1) {
                    return builder.notEqual(index, values.get(0));
                }
                return new LeafPredicate(NotIn.INSTANCE, field.type(), index, field.name(), values);
            default:
                throw new UnsupportedOperationException("Unsupported InOp: " + filter.getInOp());
        }
    }

    private static Predicate toPaimonPredicate(ValueFilter filter, Header header, RowType projectedSchema) {
        PredicateBuilder builder = new PredicateBuilder(projectedSchema);

        int index = getIndex(filter.getPath(), header);
        Object value = getJavaObject(header, index, filter.getValue());
        switch (filter.getOp()) {
            case GE:
            case GE_AND:
                return builder.greaterOrEqual(index, value);
            case G:
            case G_AND:
                return builder.greaterThan(index, value);
            case LE:
            case LE_AND:
                return builder.lessOrEqual(index, value);
            case L:
            case L_AND:
                return builder.lessThan(index, value);
            case E:
            case E_AND:
                return builder.equal(index, value);
            case NE:
            case NE_AND:
                return builder.notEqual(index, value);
            case LIKE:
            case LIKE_AND:
            case NOT_LIKE:
            case NOT_LIKE_AND:
            default:
                throw new UnsupportedOperationException("Unsupported InOp: " + filter.getOp());
        }
    }

    private static Predicate toPaimonPredicate(AndFilter filter, Header header, RowType projectedSchema) {
        List<Predicate> predicates = new ArrayList<>();
        for (Filter child : filter.getChildren()) {
            predicates.add(toPaimonPredicate(child, header, projectedSchema));
        }
        if(predicates.isEmpty()) {
            throw new UnsupportedOperationException("AndFilter has no children");
        }
        return PredicateBuilder.and(predicates);
    }

    private static Predicate toPaimonPredicate(OrFilter filter, Header header, RowType projectedSchema) {
        List<Predicate> predicates = new ArrayList<>();
        for (Filter child : filter.getChildren()) {
            predicates.add(toPaimonPredicate(child, header, projectedSchema));
        }
        if(predicates.isEmpty()) {
            throw new UnsupportedOperationException("OrFilter has no children");
        }
        return PredicateBuilder.or(predicates);
    }

    private static int getIndex(String pattern, Header header) {
        List<Integer> indices = header.patternIndexOf(pattern);
        if (indices.isEmpty()) {
            throw new UnsupportedOperationException("Path pattern " + pattern + " does not match any field in header " + header);
        }
        if (indices.size() > 1) {
            throw new UnsupportedOperationException("Path pattern " + pattern + " matches multiple fields in header " + header);
        }
        return indices.get(0) + 1;
    }

    private static Object getJavaObject(Header header, int index, Value value) {
        DataType type = header.getField(index - 1).getType();
        Object result = value.getValue();
        if(result == null) {
            throw new UnsupportedOperationException("Value is null");
        }
        switch (type) {
            case INTEGER:
                if(result instanceof Integer) {
                    return result;
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to int");
            case LONG:
                if(result instanceof Long || result instanceof Integer) {
                    return ((Number) result).longValue();
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to long");
            case FLOAT:
                if(result instanceof Float || result instanceof Integer || result instanceof Long) {
                    return ((Number) result).floatValue();
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to float");
            case DOUBLE:
                if(result instanceof Number) {
                    return ((Number) result).doubleValue();
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to double");
            case BINARY:
                if (result instanceof byte[]) {
                    return BinaryString.fromBytes((byte[]) result);
                }
                throw new UnsupportedOperationException("Value " + result + " cannot be cast to byte[]");
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + type);
        }
    }

    public static Map<String, org.apache.paimon.types.DataType> extractFields(Predicate predicate) {
        Map<String, org.apache.paimon.types.DataType> fields = new HashMap<>();
        predicate.visit(new PredicateVisitor<Void>(){

            @Override
            public Void visit(LeafPredicate predicate) {
                DataField field = new DataField(predicate.index(),predicate.fieldName(), predicate.type());
                if(field.id() != 0) {
                    fields.put(field.name(), field.type());
                }
                return null;
            }

            @Override
            public Void visit(CompoundPredicate predicate) {
                for(Predicate child : predicate.children()) {
                    child.visit(this);
                }
                return null;
            }
        });
        return fields;
    }

}
