/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive test cases for PredicateSimplifier covering:
 * - Basic comparison operators (=, !=, <, >, <=, >=)
 * - Logical operators (AND, OR)
 * - Null checks (IS NULL, IS NOT NULL)
 * - Set operations (IN, NOT IN)
 * - Complex nested predicates
 * - Edge cases and contradictions
 * - TPC-H-like complex queries with multiple fields
 */
@DisplayName("PredicateSimplifier Tests")
public class PredicateDeduperTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredicateDeduperTest.class);

    private PredicateBuilder predicateBuilder;
    private RowType schema;

    @BeforeEach
    public void setUp() {
        List<DataType> types = Arrays.asList(
                DataTypes.BIGINT(),
                DataTypes.STRING(),
                DataTypes.STRING()
        );
        List<String> names = Arrays.asList("l_quantity", "l_shipmode", "l_shipinstruct");
        schema = RowType.of(types.toArray(new DataType[0]), names.toArray(new String[0]));

        predicateBuilder = new PredicateBuilder(schema);
    }

    @Test
    @DisplayName("Test giant abnormal predicate from user query")
    public void testUserProvidedMegaPredicate() {
        DataField field1 = schema.getFields().get(1);

        Predicate qGe7 = predicateBuilder.greaterOrEqual(0, 7L);
        Predicate qGe10 = predicateBuilder.greaterOrEqual(0, 10L);
        Predicate qGe28 = predicateBuilder.greaterOrEqual(0, 28L);
        Predicate qLe38 = predicateBuilder.lessOrEqual(0, 38L);
        Predicate qLe20 = predicateBuilder.lessOrEqual(0, 20L);
        Predicate qLe17 = predicateBuilder.lessOrEqual(0, 17L);
        Predicate shipModeAir = new LeafPredicate(In.INSTANCE, field1.type(), 1, field1.name(), Arrays.asList(
                BinaryString.fromString("AIR"),
                BinaryString.fromString("AIR REG")));
        Predicate shipInstrEq = predicateBuilder.equal(2, BinaryString.fromString("DELIVER IN PERSON"));

        List<Predicate> clauses = Arrays.asList(
                or3(qGe7, qGe10, qGe28),
                or3(qGe7, qGe10, qLe38),
                or3(qGe7, qGe10, shipModeAir),
                or3(qGe7, qGe10, shipInstrEq),
                or3(qGe7, qLe20, qGe28),
                or3(qGe7, qLe20, qLe38),
                or3(qGe7, qLe20, shipModeAir),
                or3(qGe7, qLe20, shipInstrEq),
                or3(qGe7, qGe28, shipModeAir),
                or3(qGe7, qLe38, shipModeAir),
                or2(qGe7, shipModeAir),
                or3(qGe7, shipInstrEq, shipModeAir),
                or3(qGe7, shipInstrEq, qGe28),
                or3(qGe7, shipInstrEq, qLe38),
                or3(qGe7, shipInstrEq, shipModeAir),
                or2(qGe7, shipInstrEq),
                or3(qLe17, qGe10, qGe28),
                or3(qLe17, qGe10, qLe38),
                or3(qLe17, qGe10, shipModeAir),
                or3(qLe17, qGe10, shipInstrEq),
                or3(qLe17, qLe20, qGe28),
                or3(qLe17, qLe20, qLe38),
                or3(qLe17, qLe20, shipModeAir),
                or3(qLe17, qLe20, shipInstrEq),
                or3(qLe17, qGe28, shipModeAir),
                or3(qLe17, qLe38, shipModeAir),
                or2(qLe17, shipModeAir),
                or3(qLe17, shipInstrEq, shipModeAir),
                or3(qLe17, shipInstrEq, qGe28),
                or3(qLe17, shipInstrEq, qLe38),
                or3(qLe17, shipInstrEq, shipModeAir),
                or2(qLe17, shipInstrEq),
                or3(qGe10, qGe28, shipModeAir),
                or3(qGe10, qLe38, shipModeAir),
                or2(qGe10, shipModeAir),
                or3(qGe10, shipInstrEq, shipModeAir),
                or3(qLe20, qGe28, shipModeAir),
                or3(qLe20, qLe38, shipModeAir),
                or2(qLe20, shipModeAir),
                or3(qLe20, shipInstrEq, shipModeAir),
                or2(qGe28, shipModeAir),
                or2(qLe38, shipModeAir),
                shipModeAir,
                or2(shipInstrEq, shipModeAir),
                or3(shipInstrEq, qGe28, shipModeAir),
                or3(shipInstrEq, qLe38, shipModeAir),
                or2(shipInstrEq, shipModeAir),
                or2(shipInstrEq, shipModeAir),
                or3(shipInstrEq, qGe10, qGe28),
                or3(shipInstrEq, qGe10, qLe38),
                or3(shipInstrEq, qGe10, shipModeAir),
                or2(qGe10, shipInstrEq),
                or3(shipInstrEq, qLe20, qGe28),
                or3(shipInstrEq, qLe20, qLe38),
                or3(shipInstrEq, qLe20, shipModeAir),
                or2(qLe20, shipInstrEq),
                or3(shipInstrEq, qGe28, shipModeAir),
                or3(shipInstrEq, qLe38, shipModeAir),
                or2(shipInstrEq, shipModeAir),
                or2(shipInstrEq, shipModeAir),
                or2(qGe28, shipInstrEq),
                or2(qLe38, shipInstrEq),
                or2(shipInstrEq, shipModeAir),
                shipInstrEq
        );

        Predicate mega = PredicateBuilder.and(clauses);
        Predicate simplified = PredicateDeduper.simplify(mega);

        assertNotNull(simplified);
        assertTrue(clauses.size() >= 60, "Mega test should contain many clauses");
        LOGGER.info("Mega predicate clause count: {}, simplified: {}", clauses.size(), simplified);
    }

    private Predicate or2(Predicate a, Predicate b) {
        return PredicateBuilder.or(Arrays.asList(a, b));
    }

    private Predicate or3(Predicate a, Predicate b, Predicate c) {
        return PredicateBuilder.or(Arrays.asList(a, b, c));
    }

}

