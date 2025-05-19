/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.enhanced.dynamodb;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.enhanced.dynamodb.xspec.ExpressionSpecBuilder.S;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.Record;
import software.amazon.awssdk.enhanced.dynamodb.xspec.ExpressionSpecBuilder;
import software.amazon.awssdk.enhanced.dynamodb.xspec.QueryEnhancedExpressionSpec;
import software.amazon.awssdk.enhanced.dynamodb.xspec.QueryExpressionSpec;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

public class XspecQueryIntegrationTest extends DynamoDbEnhancedIntegrationTestBase {

    private static final String TABLE_NAME = createTestTableName();

    private static DynamoDbEnhancedClient enhancedClient;
    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbTable<Record> mappedTable;

    @BeforeClass
    public static void setup() {
        dynamoDbClient = createDynamoDbClient();
        enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();
        mappedTable = enhancedClient.table(TABLE_NAME, TABLE_SCHEMA);
        mappedTable.createTable();
        dynamoDbClient.waiter().waitUntilTableExists(r -> r.tableName(TABLE_NAME));
    }

    @AfterClass
    public static void teardown() {
        try {
            dynamoDbClient.deleteTable(r -> r.tableName(TABLE_NAME));
        } finally {
            dynamoDbClient.close();
        }
    }

    private void insertTestRecords() {
        List<Record> records = IntStream.range(0, 10)
                                        .mapToObj(i -> new Record()
                                            .setId("xspec-id")
                                            .setSort(i)
                                            .setStringAttribute(i % 2 == 0 ? "match" : "skip"))
                                        .collect(Collectors.toList());

        records.forEach(mappedTable::putItem);
    }

    @Test
    public void query_withQueryEnhancedExpressionSpec() {
        insertTestRecords();

        QueryEnhancedExpressionSpec spec = new ExpressionSpecBuilder()
            .withKeyCondition(S("id").eq("xspec-id"))
            .withCondition(S("stringAttribute").eq("match"))
            .addProjections("id", "sort", "stringAttribute")
            .buildForQueryEnhanced();

        QueryEnhancedRequest request = QueryEnhancedRequest.builder()
                                                           .queryConditional(QueryConditional.keyEqualTo(k -> k.partitionValue("xspec-id")))
                                                           .filterExpression(spec.getFilterExpression())
                                                           .attributesToProject(spec.getAttributesToProject())
                                                           .addNestedAttributesToProject(spec.getNestedAttributesToProject())
                                                           .build();

        List<Page<Record>> pages = mappedTable.query(request).stream().collect(Collectors.toList());

        assertThat(pages).hasSize(1);
        assertThat(pages.get(0).items()).hasSize(5);
    }

    @Test
    public void query_withQueryExpressionSpec_lowLevel() {
        insertTestRecords();

        QueryExpressionSpec spec = new ExpressionSpecBuilder()
            .withKeyCondition(S("id").eq("xspec-id"))
            .withCondition(S("stringAttribute").eq("match"))
            .addProjections("id", "sort", "stringAttribute")
            .buildForQuery();

        QueryRequest request = QueryRequest.builder()
                                           .tableName(TABLE_NAME)
                                           .keyConditionExpression(spec.getKeyConditionExpression())
                                           .filterExpression(spec.getFilterExpression())
                                           .projectionExpression(spec.getProjectionExpression())
                                           .expressionAttributeNames(spec.getExpressionNames())
                                           .expressionAttributeValues(spec.getExpressionValues())
                                           .build();

        List<Map<String, AttributeValue>> items =
            dynamoDbClient.query(request).items();

        assertThat(items).hasSize(5);
        assertThat(items.get(0)).containsKeys("id", "sort", "stringAttribute");
    }
}
