/*
 * Copyright 2015-2025 Amazon Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://aws.amazon.com/apache2.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.awssdk.enhanced.dynamodb.xspec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.NestedAttributeName;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.AttributeValueMapConverter;

/**
 * A request-centric Expression Specification Builder that can be used to construct valid expressions, and the respective name
 * maps and value maps, for various DynamoDB requests in a type manner. This includes <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >Update expression</a>, <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html" >Condition
 * expression</a> (including Filter expression and Key Condition expression), and <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html" >Projection
 * expression</a>. This class is the API entry point to this library.
 * <p>
 * This builder object is not thread-safe, but you can reuse or build on (the specific states of) a builder by cloning it into
 * separate instances for use in a concurrent environment.
 *
 * <h2>Sample Usage 1: Conditional Updates with Expressions</h2>
 *
 * <pre class="brush: java">
 * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
 * ...
 * Table table = dynamo.getTable(TABLE_NAME);
 *
 * UpdateItemExpressionSpec xspec = new ExpressionSpecBuilder()
 *      // SET num1 = num1 + 20
 *      .addUpdate(
 *          N("num1").set(N("num1").plus(20)))
 *      // SET string-attr = "string-value"
 *      .addUpdate(
 *          S("string-attr").set("string-value")
 *      )
 *      // num2 BETWEEN 0 AND 100
 *      .withCondition(
 *          N("num2").between(0, 100)
 *      ).buildForUpdate();
 *
 * table.updateItem(HASH_KEY_NAME, "hashKeyValue", RANGE_KEY_NAME, 0, xspec);
 * </pre>
 *
 * <h2>Sample Usage 2: Conditional Updates with complex Condition Expression</h2>
 * <p>
 * Let's say you want to include a complex condition expression such as:
 *
 * <pre>
 *   (attribute_not_exists(item_version) AND attribute_not_exists(config_id) AND attribute_not_exists(config_version)) OR
 *   (item_version < 123) OR
 *   (item_version = 123 AND config_id < 456) OR
 *   (item_version = 123 AND config_id = 456 AND config_version < 999)
 * </pre>
 * <p>
 * Here is how:
 * <p>
 *
 * <pre class="brush: java">
 * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
 * ...
 * Table table = dynamo.getTable(TABLE_NAME);
 *
 * UpdateItemExpressionSpec xspec = new ExpressionSpecBuilder()
 *      // SET num1 = num1 + 20
 *      .addUpdate(
 *          N("num1").set(N("num1").plus(20)))
 *      // SET string-attr = "string-value"
 *      .addUpdate(
 *          S("string-attr").set("string-value")
 *      )
 *      // a complex condition expression (as shown above)
 *      .withCondition(
 *          // add explicit parenthesis
 *          parenthesize( attribute_not_exists("item_version")
 *              .and( attribute_not_exists("config_id") )
 *              .and( attribute_not_exists("config_version") )
 *          ).or( N("item_version").lt(123) )
 *           .or( N("item_version").eq(123)
 *              .and( N("config_id").lt(456) ) )
 *           .or( N("item_version").eq(123)
 *              .and( N("config_id").eq(456) )
 *              .and( N("config_version").lt(999) ))
 *      ).buildForUpdate();
 *
 * table.updateItem(HASH_KEY_NAME, "hashKeyValue", RANGE_KEY_NAME, 0, xspec);
 * </pre>
 *
 * <h2>Sample Usage 3: Scan with Filter Expression</h2>
 * <p>
 * Without ExpressionSpecBuilder, the code (using the DynamoDB Document API) could be something like:
 *
 * <pre class="brush: java">
 * ItemCollection&lt;?&gt; col = table.scan(
 *         &quot;(#hk = :hashkeyAttrValue) AND (#rk BETWEEN :lo AND :hi)&quot;,
 *         new NameMap().with(&quot;#hk&quot;, HASH_KEY_NAME).with(&quot;#rk&quot;, RANGE_KEY_NAME),
 *         new ValueMap().withString(&quot;:hashkeyAttrValue&quot;, &quot;allDataTypes&quot;)
 *                 .withInt(&quot;:lo&quot;, 1).withInt(&quot;:hi&quot;, 10));
 * </pre>
 * <p>
 * In contrast, using ExpressionSpecBuilder:
 * <p>
 *
 * <pre class="brush: java">
 * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
 * ...
 * ScanExpressionSpec xspec = new ExpressionSpecBuilder()
 *     .withCondition(
 *         S(HASH_KEY_NAME).eq("allDataTypes")
 *             .and(N(RANGE_KEY_NAME).between(1, 10))
 * ).buildForScan();
 *
 * ItemCollection<?> col = table.scan(xspec);
 * </pre>
 *
 * <h2>Sample Usage 4: Updates with SET, ADD, DELETE and REMOVE</h2>
 *
 * <pre class="brush: java">
 * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
 * ...
 * Table table = dynamo.getTable(TABLE_NAME);
 *
 * UpdateItemExpressionSpec xspec = new ExpressionSpecBuilder()
 *     .addUpdate(S("mapAttr.colors[0]").set("red"))
 *     .addUpdate(S("mapAttr.colors[1]").set("blue"))
 *     .addUpdate(L("mapAttr.members").set(
 *         L("mapAttr.members").listAppend("marry", "liza")))
 *     .addUpdate(SS("mapAttr.countries").append("cn", "uk"))
 *     .addUpdate(SS("mapAttr.brands").delete("Facebook", "LinkedIn"))
 *     .addUpdate(attribute("mapAttr.foo").remove())
 *     .buildForUpdate();
 *
 * assertEquals("SET #0.#1[0] = :0, #0.#1[1] = :1, #0.#2 = list_append(#0.#2, :2) ADD #0.#3 :3 DELETE #0.#4 :4 REMOVE #0.#5",
 *     xspec.getUpdateExpression());
 *
 * final String hashkey = "addRemoveDeleteColors";
 * table.updateItem(HASH_KEY_NAME, hashkey, RANGE_KEY_NAME, 0, xspec);
 * </pre>
 *
 * @see PathOperand
 */
public final class ExpressionSpecBuilder implements Cloneable {
    private final Map<String, List<UpdateAction>> updates;
    private Condition keyCondition;
    private Condition condition;
    private final Set<PathOperand> projections;

    /**
     * Constructs a request-centric Expression Specification Builder that can be used to construct valid expressions, and the
     * respective name maps and value maps, for various DynamoDB requests in a typeful manner. This includes <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >Update expression</a>, <a
     * href= "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html" >Condition
     * expression</a> (including Filter expression and Key Condition expression), and <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html" >Projection
     * expression</a>. This class is the API entry point to this library.
     * <p>
     * This builder object is not thread-safe, but you can reuse or build on (the specific states of) a builder by cloning it into
     * separate instances for use in a concurrent environment.
     * <h4>Sample Usage: Query with Filter Expression</h4>
     * <p>
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     * Table table = dynamo.getTable(TABLE_NAME);
     *
     * QueryExpressionSpec xspec = new ExpressionSpecBuilder()
     *     .addProjections("numberAttr", "stringAttr")
     *     .withCondition(BOOL("booleanTrue").eq(true)
     *                    .and(S("mapAttr.key1").eq("value1"))
     *  ).buildForQuery();
     *
     *  ItemCollection<?> col = table.query(HASH_KEY_NAME, "allDataTypes",
     *      new RangeKeyCondition("range_key_name").between(1, 10), xspec);
     * </pre>
     *
     * <h4>Sample Usage: Conditional Updates with Expressions</h4>
     *
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     * Table table = dynamo.getTable(TABLE_NAME);
     *
     * UpdateItemExpressionSpec xspec = new ExpressionSpecBuilder()
     *      // SET num1 = num1 + 20
     *      .addUpdate(
     *          N("num1").set(N("num1").plus(20)))
     *      // SET string-attr = "string-value"
     *      .addUpdate(
     *          S("string-attr").set("string-value")
     *      )
     *      // num2 BETWEEN 0 AND 100
     *      .withCondition(
     *          N("num2").between(0, 100)
     *      ).buildForUpdate();
     *
     * table.updateItem(HASH_KEY_NAME, "hashKeyValue", RANGE_KEY_NAME, 0, xspec);
     * </pre>
     * <h4>Sample Usage: Scan with Filter Expression</h4>
     * <p>
     * Without ExpressionSpecBuilder, the code (using the DynamoDB Document API) could be something like:
     *
     * <pre class="brush: java">
     * ItemCollection&lt;?&gt; col = table.scan(
     *         &quot;(#hk = :hashkeyAttrValue) AND (#rk BETWEEN :lo AND :hi)&quot;,
     *         new NameMap().with(&quot;#hk&quot;, HASH_KEY_NAME).with(&quot;#rk&quot;, RANGE_KEY_NAME),
     *         new ValueMap().withString(&quot;:hashkeyAttrValue&quot;, &quot;allDataTypes&quot;)
     *                 .withInt(&quot;:lo&quot;, 1).withInt(&quot;:hi&quot;, 10));
     * </pre>
     * <p>
     * In contrast, using ExpressionSpecBuilder:
     * <p>
     *
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     * ScanExpressionSpec xspec = new ExpressionSpecBuilder()
     *     .withCondition(
     *         S(HASH_KEY_NAME).eq("allDataTypes")
     *             .and(N(RANGE_KEY_NAME).between(1, 10))
     * ).buildForScan();
     *
     * ItemCollection<?> col = table.scan(xspec);
     * </pre>
     *
     * <h4>Sample Usage: Conditional Updates with Expressions</h4>
     *
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     * Table table = dynamo.getTable(TABLE_NAME);
     *
     * UpdateItemExpressionSpec xspec = new ExpressionSpecBuilder()
     *      // SET num1 = num1 + 20
     *      .addUpdate(
     *          N("num1").set(N("num1").plus(20)))
     *      // SET string-attr = "string-value"
     *      .addUpdate(
     *          S("string-attr").set("string-value")
     *      )
     *      // num2 BETWEEN 0 AND 100
     *      .withCondition(
     *          N("num2").between(0, 100)
     *      ).buildForUpdate();
     *
     * table.updateItem(HASH_KEY_NAME, "hashKeyValue", RANGE_KEY_NAME, 0, xspec);
     * </pre>
     *
     * <h4>Sample Usage: Conditional Updates with complex Condition Expression</h4>
     * <p>
     * Let's say you want to include a complex condition expression such as:
     *
     * <pre>
     *   (attribute_not_exists(item_version) AND attribute_not_exists(config_id) AND attribute_not_exists(config_version)) OR
     *   (item_version < 123) OR
     *   (item_version = 123 AND config_id < 456) OR
     *   (item_version = 123 AND config_id = 456 AND config_version < 999)
     * </pre>
     * <p>
     * Here is how:
     * <p>
     *
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     * Table table = dynamo.getTable(TABLE_NAME);
     *
     * UpdateItemExpressionSpec xspec = new ExpressionSpecBuilder()
     *      // SET num1 = num1 + 20
     *      .addUpdate(
     *          N("num1").set(N("num1").plus(20)))
     *      // SET string-attr = "string-value"
     *      .addUpdate(
     *          S("string-attr").set("string-value")
     *      )
     *      // a complex condition expression (as shown above)
     *      .withCondition(
     *          // add explicit parenthesis
     *          parenthesize( attribute_not_exists("item_version")
     *              .and( attribute_not_exists("config_id") )
     *              .and( attribute_not_exists("config_version") )
     *          ).or( N("item_version").lt(123) )
     *           .or( N("item_version").eq(123)
     *              .and( N("config_id").lt(456) ) )
     *           .or( N("item_version").eq(123)
     *              .and( N("config_id").eq(456) )
     *              .and( N("config_version").lt(999) ))
     *      ).buildForUpdate();
     *
     * table.updateItem(HASH_KEY_NAME, "hashKeyValue", RANGE_KEY_NAME, 0, xspec);
     * </pre>
     *
     * <h4>Sample Usage: Updates with SET, ADD, DELETE and REMOVE</h4>
     *
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     * Table table = dynamo.getTable(TABLE_NAME);
     *
     * UpdateItemExpressionSpec xspec = new ExpressionSpecBuilder()
     *     .addUpdate(S("mapAttr.colors[0]").set("red"))
     *     .addUpdate(S("mapAttr.colors[1]").set("blue"))
     *     .addUpdate(L("mapAttr.members").set(
     *         L("mapAttr.members").listAppend("marry", "liza")))
     *     .addUpdate(SS("mapAttr.countries").append("cn", "uk"))
     *     .addUpdate(SS("mapAttr.brands").delete("Facebook", "LinkedIn"))
     *     .addUpdate(attribute("mapAttr.foo").remove())
     *     .buildForUpdate();
     *
     * assertEquals("SET #0.#1[0] = :0, #0.#1[1] = :1, #0.#2 = list_append(#0.#2, :2) ADD #0.#3 :3 DELETE #0.#4 :4 REMOVE #0.#5",
     *     xspec.getUpdateExpression());
     *
     * final String hashkey = "addRemoveDeleteColors";
     * table.updateItem(HASH_KEY_NAME, hashkey, RANGE_KEY_NAME, 0, xspec);
     * </pre>
     *
     * @see PathOperand
     */
    public ExpressionSpecBuilder() {
        this.updates = new LinkedHashMap<>();
        this.projections = new LinkedHashSet<>();
    }

    private ExpressionSpecBuilder(ExpressionSpecBuilder from) {
        this.updates = new LinkedHashMap<>(from.updates);
        this.projections = new LinkedHashSet<>(from.projections);
        this.keyCondition = from.keyCondition;
        this.condition = from.condition;
    }

    /**
     * Fluent API to add the given Update expression for a request.
     * <p>
     * For example:
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     * builder
     *      // SET num1 = num1 + 20
     *     .addUpdate(
     *         N("num1").set(N("num1").plus(20)))
     *      // SET string-attr = "string-value"
     *     .addUpdate(
     *         S("string-attr").set("string-value")
     * )
     * </pre>
     */
    public ExpressionSpecBuilder addUpdate(UpdateAction updateAction) {
        String operator = updateAction.getOperator();
        List<UpdateAction> list = updates.computeIfAbsent(operator, k -> new LinkedList<>());
        list.add(updateAction);
        return this;
    }

    /**
     * Fluent API to set the condition expression for a request.
     * <p>
     * For example:
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     *     builder.withCondition(
     *         // num2 BETWEEN 0 AND 100
     *         ExpressionSpecBuilder.N("num2").between(0, 100)
     *     )
     * ...
     * </pre>
     * Example of specifying a complex condition:
     * <pre class="brush: java">
     * import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.*;
     * ...
     *      // A complex condition expression:
     *      //
     *      // (attribute_not_exists(item_version) AND attribute_not_exists(config_id) AND attribute_not_exists(config_version)) OR
     *      // (item_version < 123) OR
     *      // (item_version = 123 AND config_id < 456) OR
     *      // (item_version = 123 AND config_id = 456 AND config_version < 999)
     *      //
     *      builder.withCondition(
     *          // add explicit parenthesis
     *          parenthesize( attribute_not_exists("item_version")
     *              .and( attribute_not_exists("config_id") )
     *              .and( attribute_not_exists("config_version") )
     *          ).or( N("item_version").lt(123) )
     *           .or( N("item_version").eq(123)
     *              .and( N("config_id").lt(456) ) )
     *           .or( N("item_version").eq(123)
     *              .and( N("config_id").eq(456) )
     *              .and( N("config_version").lt(999) ))
     *      )
     * ...
     * </pre>
     */
    public ExpressionSpecBuilder withCondition(Condition condition) {
        this.condition = condition;
        return this;
    }

    public ExpressionSpecBuilder withKeyCondition(Condition keyCondition) {
        this.keyCondition = keyCondition;
        return this;
    }

    /**
     * Fluent API to add the given attribute to the list of projection of a request. For example:
     * <pre class="brush: java">
     * builder.addProjection("binarySetAttribute")
     *        .addProjection("listAttr[0]")
     *        .addProjection("mapAttr.name")
     *        .addProjection("stringSetAttr")
     *        ;
     * </pre>
     */
    public ExpressionSpecBuilder addProjection(String path) {
        projections.add(new PathOperand(path));
        return this;
    }

    /**
     * Fluent API to add the given attributes to the list of projection of a request. For example:
     * <pre class="brush: java">
     * builder.addProjections("binarySetAttribute", "listAttr[0]", "mapAttr.name", "stringSetAttr");
     * </pre>
     */
    public ExpressionSpecBuilder addProjections(String... paths) {
        for (String path : paths) {
            addProjection(path);
        }
        return this;
    }

    /**
     * Returns an expression specification for use in a {@code DeleteItem} request to DynamoDB.
     */
    public DeleteItemExpressionSpec buildForDeleteItem() {
        return new DeleteItemExpressionSpec(this);
    }

    /**
     * Returns an expression specification for use in a {@code GetItem} request to DynamoDB.
     */
    public GetItemExpressionSpec buildForGetItem() {
        return new GetItemExpressionSpec(this);
    }

    /**
     * Returns an expression specification for use in a query request to DynamoDB.
     */
    public QueryExpressionSpec buildForQuery() {
        return new QueryExpressionSpec(this);
    }

    /**
     * Returns an expression specification for use in a scan request to DynamoDB.
     */
    public ScanExpressionSpec buildForScan() {
        return new ScanExpressionSpec(this);
    }

    /**
     * Returns an expression specification for use in an {@code UpdateItem} request to DynamoDB.
     */
    public UpdateItemExpressionSpec buildForUpdate() {
        return new UpdateItemExpressionSpec(this);
    }

    /**
     * Returns an expression specification for use in a {@code PutItem} request to DynamoDB.
     */
    public PutItemExpressionSpec buildForPut() {
        return new PutItemExpressionSpec(this);
    }

    /**
     * Builds and returns the update expression to be used in a dynamodb request; or null if there is none.
     */
    Expression buildUpdateExpression(SubstitutionContext context) {
        StringBuilder sb = new StringBuilder();
        context.beginTrackingAccess();
        updates.forEach((operator, value) -> {
            boolean firstOfUpdateType = true;
            for (UpdateAction expr : value) {
                if (firstOfUpdateType) {
                    firstOfUpdateType = false;
                    if (sb.length() > 0) {
                        sb.append(" ");
                    }
                    sb.append(operator).append(" ");
                } else {
                    sb.append(", ");
                }
                sb.append(expr.asSubstituted(context));
            }
        });

        return Expression.builder()
            .expression(sb.toString())
            .expressionNames(context.getAccessedNames() == null ? null : Collections.unmodifiableMap(context.getAccessedNames()))
            .expressionValues(context.getAccessedValues() == null ? null :
                           new AttributeValueMapConverter().convert(context.getAccessedValues()))
            .build();
    }

    /**
     * Builds and returns the projection expression to be used in a dynamodb GetItem request; or null if there is none.
     */

    Expression buildProjectionExpression(SubstitutionContext context) {
        if (projections.isEmpty()) {
            return null;
        }
        context.beginTrackingAccess();
        String projectionExpression = projections.stream()
                                                 .map(projection -> projection.asSubstituted(context))
                                                 .collect(Collectors.joining(", "));
        return Expression.builder()
                         .expression(projectionExpression)
                         .expressionNames(context.getAccessedNames() == null ? null : Collections.unmodifiableMap(context.getAccessedNames()))
                         .build();
    }

    List<String> buildAttributesToProject() {
        if (projections == null || projections.isEmpty()) {
            return Collections.emptyList();
        }

        return projections.stream()
                          .map(PathOperand::getPath)
                          .filter(path -> !path.contains(".")) // top-level only
                          .distinct()
                          .collect(Collectors.toList());
    }

    public List<NestedAttributeName> buildNestedAttributesToProject() {
        if (projections == null || projections.isEmpty() || projections.stream()
                       .noneMatch(pathOp -> pathOp.getPath().contains("."))) {
            return Collections.emptyList();
        }

        return projections.stream()
                          .map(PathOperand::getPath)     // extract the string path: e.g., "level0.level1.level2"
                          .filter(path -> path.contains("."))
                          .distinct()
                          .map(path -> Arrays.asList(path.split("\\.")))    // split by dots
                          .map(NestedAttributeName::create)  // build NestedAttributeName
                          .collect(Collectors.toList());
    }

    /**
     * Builds and returns the condition expression to be used in a dynamodb request; or null if there is none.
     */

    Expression buildConditionExpression(SubstitutionContext context) {
        if(condition == null) {
            return null;
        }

        context.beginTrackingAccess();
        String conditionExpression = condition.asSubstituted(context);
        return Expression.builder()
                         .expression(conditionExpression)
                         .expressionNames(context.getAccessedNames() == null ? null : Collections.unmodifiableMap(context.getAccessedNames()))
                         .expressionValues(context.getAccessedValues() == null ? null :
                                           new AttributeValueMapConverter().convert(context.getAccessedValues()))
                         .build();
    }

    /**
     * Builds and returns the key condition expression to be used in a dynamodb query request; or null if there is none.
     */

    Expression buildKeyConditionExpression(SubstitutionContext context) {
        if(keyCondition == null) {
            return null;
        }
        context.beginTrackingAccess();
        String keyConditionExpression = keyCondition.asSubstituted(context);
        return Expression.builder()
                         .expression(keyConditionExpression)
                         .expressionNames(context.getAccessedNames() == null ? null : Collections.unmodifiableMap(context.getAccessedNames()))
                         .expressionValues(context.getAccessedValues() == null ? null :
                                           new AttributeValueMapConverter().convert(context.getAccessedValues()))
                         .build();
    }

    @Override
    public ExpressionSpecBuilder clone() {
        return new ExpressionSpecBuilder(this);
    }

    ////////////// static factory methods //////////////

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param pathOperand path operand that refers to the attribute
     * @param operand     default value if the attribute doesn't exist
     */
    static <T> IfNotExistsFunction<T> if_not_exists(
        PathOperand pathOperand, Operand operand) {
        return new IfNotExistsFunction<>(pathOperand, operand);
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for number (N) attribute.
     */
    public static IfNotExistsFunction<N> if_not_exists(String path,
                                                       Number defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for binary (B) attribute.
     */
    public static IfNotExistsFunction<B> if_not_exists(String path,
                                                       byte[] defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for binary (B) attribute.
     */
    public static IfNotExistsFunction<B> if_not_exists(String path,
                                                       ByteBuffer defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for boolean (BOOL) attribute.
     */
    public static IfNotExistsFunction<BOOL> if_not_exists(String path,
                                                          boolean defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for binary set (BS) attribute.
     */
    public static IfNotExistsFunction<BS> if_not_exists(String path,
                                                        byte[]... defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for binary set (BS) attribute.
     */
    public static IfNotExistsFunction<BS> if_not_exists(String path,
                                                        ByteBuffer... defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for list (L) attribute.
     */
    public static IfNotExistsFunction<L> if_not_exists(String path,
                                                       List<?> defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for map (M) attribute.
     */
    public static IfNotExistsFunction<M> if_not_exists(String path,
                                                       Map<String, ?> defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for number set (NS) attribute.
     */
    public static IfNotExistsFunction<NS> if_not_exists(String path,
                                                        Number... defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for string (S) attribute.
     */
    public static IfNotExistsFunction<S> if_not_exists(String path,
                                                       String defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param path         document path to an attribute
     * @param defaultValue default value if the attribute doesn't exist
     * @return an {@code IfNotExists} object for string set (SS) attribute.
     */
    public static IfNotExistsFunction<SS> if_not_exists(String path,
                                                        String... defaultValue) {
        return if_not_exists(new PathOperand(path), new LiteralOperand(
            defaultValue));
    }

    /**
     * Returns a {@code ListAppend} object which represents a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >list_append(operand,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "list_append(operand, operand) – This function evaluates to a list with a
     * new element added to it. You can append the new element to the start or
     * the end of the list by reversing the order of the operands."
     * </pre>
     *
     * @param path  document path to a list attribute
     * @param value single value to be appended to the list attribute
     */
    public static <T> ListAppendFunction list_append(String path, T value) {
        LinkedList<T> list = new LinkedList<>();
        list.add(value);
        return list_append(path, list);
    }

    /**
     * Returns a {@code ListAppend} object which represents a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >list_append(operand,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "list_append(operand, operand) – This function evaluates to a list with a
     * new element added to it. You can append the new element to the start or
     * the end of the list by reversing the order of the operands."
     * </pre>
     *
     * @param path  document path to a list attribute
     * @param value list of values to be appended to the list attribute
     */
    public static <T> ListAppendFunction list_append(String path,
                                                     List<? extends T> value) {
        return new ListAppendFunction(L(path), new ListLiteralOperand(new LinkedList<T>(
            value)));
    }

    /**
     * Returns a {@code ListAppend} object which represents a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >list_append(operand,
     * operand)</a> function call; used for building expression.
     *
     * <pre>
     * "list_append(operand, operand) – This function evaluates to a list with a
     * new element added to it. You can append the new element to the start or
     * the end of the list by reversing the order of the operands."
     * </pre>
     *
     * @param value list of values to be appended to
     * @param path  document path to a list attribute
     */
    public static <T> ListAppendFunction list_append(List<? extends T> value,
                                                     String path) {
        return new ListAppendFunction(new ListLiteralOperand(new LinkedList<T>(value)),
                                      L(path));
    }

    // ///////////////////////// FunctionCondition factory methods

    /**
     * Returns a <a href= "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions" >function condition</a> (that evaluates to true if the attribute of the
     * specified path operand exists) for building condition expression.
     */
    public static <T> FunctionCondition attribute_exists(
        PathOperand pathOperand) {
        return new FunctionCondition("attribute_exists", pathOperand);
    }

    /**
     * Returns a <a href= "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions" >function condition</a> (that evaluates to true if the attribute at the
     * specified path exists) for building condition expression.
     */
    public static <T> FunctionCondition attribute_exists(String path) {
        return attribute_exists(new PathOperand(path));
    }

    /**
     * Returns a <a href= "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions" >function condition</a> (that evaluates to true if the attribute of the
     * specified path operand does not exist) for building condition expression.
     */
    public static FunctionCondition attribute_not_exists(
        PathOperand pathOperand) {
        return new FunctionCondition("attribute_not_exists", pathOperand);
    }

    /**
     * Returns a <a href= "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions" >function condition</a> (that evaluates to true if the attribute at the
     * specified path does not exist) for building condition expression.
     */
    public static FunctionCondition attribute_not_exists(String path) {
        return attribute_not_exists(new PathOperand(path));
    }

    /**
     * Returns a <a href= "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions" >negation</a> of the specified condition; used for building condition
     * expression.
     */
    public static <T> NegationCondition not(Condition cond) {
        return new NegationCondition(cond);
    }

    /**
     * Returns a {@code RemoveAction} for removing the attribute with the specified path from an item; used for building
     * update expression.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static RemoveAction remove(String path) {
        return new PathOperand(path).remove();
    }

    /**
     * Returns a path operand that refers to an attribute of some unspecified
     * <a href="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html">data type</a>; used for
     * building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static PathOperand attribute(String path) {
        return new PathOperand(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >boolean attribute</a> for the
     * purpose of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static BOOL BOOL(String path) {
        return new BOOL(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >NULL attribute</a> for the purpose
     * of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static NULL NULL(String path) {
        return new NULL(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >binary attribute</a> for the
     * purpose of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static B B(String path) {
        return new B(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >number attribute</a> for the
     * purpose of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static N N(String path) {
        return new N(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >string attribute</a> for the
     * purpose of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static S S(String path) {
        return new S(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >binary-set attribute</a> for the
     * purpose of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static BS BS(String path) {
        return new BS(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >number-set attribute</a> for the
     * purpose of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static NS NS(String path) {
        return new NS(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >string-set attribute</a> for the
     * purpose of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static SS SS(String path) {
        return new SS(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >list attribute</a> for the purpose
     * of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static L L(String path) {
        return new L(path);
    }

    /**
     * Creates a path operand that refers to a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >map attribute</a> for the purpose
     * of building expressions.
     *
     * @param path the document path to the attribute, where nested path elements are assumed to be delimited by either "." or
     *             array indexing such as "[1]".
     */
    public static M M(String path) {
        return new M(path);
    }

    /**
     * Returns an explicitly parenthesized condition, ie '(' condition ')' used in building condition expressions.
     *
     * @see #wrapCondition(Condition)
     */
    public static <T> ParenthesizedCondition parenthesize(Condition condition) {
        return ParenthesizedCondition.getInstance(condition);
    }

    /**
     * A shorthand for calling {@link #parenthesize(Condition)} to explicitly parenthesize a given condition for building
     * condition expressions.
     */
    public static <T> ParenthesizedCondition wrapCondition(Condition condition) {
        return parenthesize(condition);
    }
}
