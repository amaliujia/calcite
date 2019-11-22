/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;


/**
 * DESCRIPTOR appears as an argument in a table-valued function that also accepts
 * TABLE parameter or subquery.
 *
 * A typical syntax is
 *    1. table_valued_func(TABLE table_name, DESCRIPTOR(col_name, ...)).
 *    2. table_valued_func((subquery), DESCRIPTOR(col_name, ...)).
 *
 * The scope of column names is within the scope of the function. Typically
 * column names can be found from output columns of TABLE parameter or subquery.
 */
public class SqlDescriptorOperator extends SqlOperator {
  public SqlDescriptorOperator() {
    super("DESCRIPTOR", SqlKind.DESCRIPTOR, 100, 100, null, null, null);
  }

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    List<SqlNode> sqlIdentifiers = call.getOperandList();

    // validate column names that are specified by DESCRIPTOR.
    sqlIdentifiers.stream().forEach(
        node -> {
          if (!(node instanceof SqlIdentifier)) {
            throw SqlUtil.newContextException(node.getParserPosition(),
                RESOURCE.aliasMustBeSimpleIdentifier());
          }

          SqlIdentifier identifier = (SqlIdentifier) node;
          if (scope.resolveColumn(identifier.getSimple(), node) == null) {
            throw SqlUtil.newContextException(node.getParserPosition(),
                RESOURCE.unknownIdentifier(identifier.getSimple()));
          }
        }
    );
    return validator.getTypeFactory().createSqlType(SqlTypeName.COLUMN_LIST);
  }

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    return opBinding.typeFactory.createSqlType(SqlTypeName.COLUMN_LIST);
  }
}

// End SqlDescriptorOperator.java
