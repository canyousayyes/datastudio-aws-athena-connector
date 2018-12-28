function generateAthenaQuery(request) {
  var columns = request.fields.map(function (field) {
    return '"' + field.name + '"';
  });
  var table = request.configParams.tableName;
  var rowLimit = parseInt(request.configParams.rowLimit);

  var query = 'SELECT ' + columns.join(', ') + ' FROM "' + table + '"';
  if (rowLimit) {
    query += ' LIMIT ' + rowLimit;
  }

  return query;
}

function runAthenaQuery(region, database, query, outputLocation) {
  var payload = {
    'ClientRequestToken': uuidv4(),
    'QueryExecutionContext': {
      'Database': database
    },
    'QueryString': query,
    'ResultConfiguration': {
      'OutputLocation': outputLocation
    }
  };
  return AWS.post('athena', region, 'AmazonAthena.StartQueryExecution', payload);
}

function waitAthenaQuery(region, queryExecutionId) {
  var payload = {
    'QueryExecutionId': queryExecutionId
  };

  // Ping for status until the query reached a terminal state
  while (1) {
    var result = AWS.post('athena', region, 'AmazonAthena.GetQueryExecution', payload);
    var state = result.QueryExecution.Status.State.toLowerCase();
    switch (state) {
      case 'succeeded':
        return true;
      case 'failed':
        throw new Error(result.QueryExecution.Status.StateChangeReason || 'Unknown query error');
      case 'cancelled':
        throw new Error('Query cancelled');
    }

    Utilities.sleep(3000);
  }
}

function getAthenaQueryResults(region, queryExecutionId) {
  // Get results until all rows are fetched
  var rows = [];
  var nextToken = null;
  while (1) {
    var payload = {
      'QueryExecutionId': queryExecutionId
    };
    if (nextToken) {
      payload.NextToken = nextToken;
    }

    // Parse and append data
    var result = AWS.post('athena', region, 'AmazonAthena.GetQueryResults', payload);
    var columns = result.ResultSet.ResultSetMetadata.ColumnInfo.map(function (info) {
      return info.Name;
    });
    result.ResultSet.Rows.forEach(function (row) {
      // Combine [val0, val1, ...] and [col0, col1, ...] into { col0: val0, col1: val1, ... }
      var newRow = {};
      row.Data.forEach(function (data, index) {
        var column = columns[index];
        newRow[column] = data.VarCharValue;
      });
      rows.push(newRow);
    });

    nextToken = result.NextToken;
    if (!nextToken) {
      break;
    }
  }

  // Athena data is CSV, need to remove header row
  rows.shift();
  return rows;
}
