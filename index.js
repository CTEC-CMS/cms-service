var AWS = require('aws-sdk');
const util = require('./util');
var s3 = new AWS.S3();
const docClient = new AWS.DynamoDB.DocumentClient({ 'region': 'us-east-2' })

exports.handler = function (event, context, callback) {
    var srcBucket = event.Records[0].s3.bucket.name;
    var srcKey = event.Records[0].s3.object.key;
    var randomNumber = 1000000;
    var validDataFormat = false;
    var invalidTINList = [];
    var invalidNPIList = [];
    var invalidModelTypeList = [];
    var invalidValidThroughDate = []
    var TOTAL_NO_OF_RECORDS_TO_BE_PROCESSED = 0;
    var TABLE_NAME = 'CMS_PROVIDER_MATCHING_REQUEST';
    
    function getFolderName() {
        var today = util.getCurrentDateTime();
        return srcBucket + '/' + today + '/success';
    }

    function getErrorFolderName() {
        var today = util.getCurrentDateTime();
        return srcBucket + '/' + today + '/error';
    }

    var storageBucketName = getFolderName();
    var storageBucketNameError = getErrorFolderName();

    var storageBucket = new AWS.S3({
        params: {
            Bucket: storageBucketName
        }
    });

    // Create a record in dynamodb //
    function updateDb( itemParam ){
        var params = {
            TableName: TABLE_NAME,
            Item: itemParam
        }
        console.log ( ' dynamodb params ' + JSON.stringify(params));
        docClient.put(params, function (err, data) {
            if (err) {
                console.log('Error while creating a record in  dynamodb ' + err);
            }
            else {
                console.log('Successfully created a record in dynamodb' + JSON.stringify(data));
            }
        });
    }

    // Read a string of CSV data and convert it into an array of JSON Objects
    function CSVToArray(strData) {
        var lines = strData.split('\n');
        var objArray = [];
        var lineList = []
        // Creating a two dimensional array lines[columns[]] from the given CSV file //
        TOTAL_NO_OF_RECORDS_TO_BE_PROCESSED = lines.length;
        for (var g = 0; g < TOTAL_NO_OF_RECORDS_TO_BE_PROCESSED; g++) {
            lines[g] = lines[g].replace(/\r?\n|\r/g, '');
            var columns = lines[g].split(',');
            var columnsList = [];
            for (var j = 0; j < columns.length; j++) {
                columnsList.push('' + columns[j]);
            }
            lineList.push(columnsList);
        }

        // Create an array of JSON Object - Object keys are CSV Header record//
        for (var i = 1; i < lineList.length; i++) {
            objArray[i - 1] = {};
            var headerRow = lineList[0]; // NPI, TIN, ModelType, Valid Through Date //
            for (var k = 0; k < lineList[0].length && k < lineList[i].length; k++) {
                var key = headerRow[k];
                objArray[i - 1][key] = lineList[i][k]
            }
        }
        return (objArray);
    }

    function validateData( dataInJsonArrayFormat ) {
        var allValidData = true;
        for( var d=0; d<dataInJsonArrayFormat.length; d++ ){
            var row = dataInJsonArrayFormat[d];
            var rowNumber = d + 2;
            var tin = '' + row.TIN;
            var npi = '' + row.NPI;
            var modelType = row['Model Type'];
            var validThroughDate = row['Valid Through Date'];
            console.log( " validating MODEL TYPE " + modelType);
            var isValidNPI = util.validateNPI(npi);
            var isValidTIN = util.validateTIN(tin);
            var isValidModelType = util.validateModelType(modelType);
            var isValidDate = util.validateDate(validThroughDate);
            if ( !isValidTIN ){
                allValidData = false;
                console.log(' invalid TIN ' + tin);
                invalidTINList.push( { 'columnName' : 'tin', 'value': tin , 'error' : 'TIN must be exactly 9 digits', 'rowNumber' : rowNumber});
            }
            if ( !isValidNPI ){
                allValidData = false;
                console.log(' invalid NPI ' + npi);
                invalidNPIList.push( { 'columnName' : 'npi', 'value': npi , 'error' : 'NPI must be exactly 10 digit', 'rowNumber' : rowNumber} );
            }
            if( !isValidModelType ) {
                allValidData = false;
                console.log(' invalid Model Type ' + modelType);
                invalidModelTypeList.push( { 'columnName' : 'modelType', 'value': modelType , 'error' : 'Model Type must contain alphabet and space', 'rowNumber' : rowNumber} );
            }
            if( !isValidDate ){
                allValidData = false;
                console.log(' invalid Valid Through Date ' + validThroughDate);
                invalidValidThroughDate.push( { 'columnName' : 'validThroughDate',  'value': validThroughDate , 'error' : 'Valid Through Date must be in mm/dd/yyyy format', 'rowNumber' : rowNumber} );
            }
        }
        return allValidData;
    }
    
    // Retrieve the object.
    s3.getObject({
        Bucket: srcBucket,
        Key: srcKey
    }, function (err, data) {
        if (err) {
            console.log(err, err.stack);
            callback(err);
        }
        else {
            var text = '' + data.Body.toString('ascii');
            var dataInJsonArrayFormat = CSVToArray(text);

            var type = 'Model Type';
            var dateName = 'Valid Through Date'
            console.log('CSV: ', dataInJsonArrayFormat);
            var modelTypeArray = dataInJsonArrayFormat.map(function (el) { return el[type]; });
            modelTypeArray = modelTypeArray.filter(function (item, index, inputArray) {
                return inputArray.indexOf(item) == index;
            });
            var statusMessage, status;
            var separateFileNameList = [];

            validDataFormat = validateData(dataInJsonArrayFormat);
            var id = 'REQ-' + util.getCurrentDateTime();
            if(!validDataFormat) {
                console.log ( ' follow error flow ');
                console.log(' Error NpiValue ' + JSON.stringify(invalidNPIList));
                console.log(' Error TinValue ' + JSON.stringify(invalidTINList));
                console.log(' Error Model Type ' + JSON.stringify(invalidModelTypeList));
                console.log(' Error Valid Through Date ' + JSON.stringify(invalidValidThroughDate));
                statusMessage = invalidNPIList.concat(invalidTINList).concat(invalidModelTypeList).concat(invalidValidThroughDate);
                // statusMessage = invalidNPIList.concat(invalidTINList).concat(invalidModelTypeList).concat(invalidValidThroughDate);
                var currentTime = new Date().toJSON();
                var dbParam = {
                    id: id,
                    storageBucketName : storageBucketNameError,
                    statusMessage: statusMessage,
                    uploadFileName: srcKey,
                    status: 'Failed',
                    outFiles: 'no record',
                    recordsProcessed: 0,
                    filesCreated: 0,
                    createdOn: currentTime
                }
                console.log ( ' DynamoDB ItemParam ' + JSON.stringify(dbParam));
                updateDb(dbParam);

                // Currently a trigger is set in S3 to process a '.csv' file 
                // changing the error file extension as .error 
                // to avoid S3 sending a notification lambda function to process any CSV file 
                var s3Params = {
                    Bucket: storageBucketNameError,
                    Key: srcKey+'.error',
                    ContentType: 'txt',
                    Body: text,
                    ACL: 'public-read'
                };
                storageBucket.putObject(s3Params, function (err, data) {
                    if( err ) {
                        console.log (' Error while copying the file to error folder ');
                    } else {
                        console.log (' Successfully moved the error file to error folder ');
                    }
                });
            }
            else {
                console.log ( ' all records are valid - process and move to the files S3 ')
                for (var h = 0; h < modelTypeArray.length - 1; h++) {
                    var currentModel = [];
                    var newFile = [];
                    currentModel = dataInJsonArrayFormat.filter(function (el) {
                        return el[type] == modelTypeArray[h];
                    });

                    for (var s = 0; s < currentModel.length; s++) {
                        ++randomNumber;
                        var tin = '' + currentModel[s].TIN;
                        var npi = '' + currentModel[s].NPI;
                        if (tin.length == 9) {
                            tin = '0' + tin;
                        }
                        var date = currentModel[s][dateName].split('/');
                        if (date[1] < 10) date[1] = '0' + date[1]; // dd
                        if (date[0] < 10) date[0] = '0' + date[0]; // mm
                        var validThroughDate = date[2] + '-' + date[0] + '-' + date[1];
                        var fileText = randomNumber + npi + tin + validThroughDate;
                        newFile.push(fileText);
                    }
                    newFile = JSON.stringify(newFile);
                    newFile = newFile.replace(/[^a-zA-Z0-9,-]/g, '');
                    newFile = newFile.replace(/,/g, '\n');
                    var splitModelName = modelTypeArray[h].replace(/\s/g, '_');
                    var formattedDate = util.getFormattedCurrentDate();
                    let fileName = splitModelName + '.' + formattedDate + '.txt';
                    var fileNameObject = {
                        'fileName': fileName,
                    }
                    separateFileNameList.push(fileNameObject);
                    var paramss = {
                        Bucket: storageBucketName,
                        Key: fileName,
                        ContentType: 'txt',
                        Body: newFile,
                        ACL: 'public-read'
                    };
                    var counter = 0;
                    storageBucket.putObject(paramss, function (err, data) {
                        counter ++;
                        if (err) {
                            console.log('failed to upload  file in S3 ', err);
                        }
                        else {
                            if ( counter === h ) {
                                console.log(' once all the files are uploaded - create a record in dynamodb');
                                var currentTime = new Date().toJSON();
                                var itemParam = {
                                    id: id,
                                    storageBucketName : storageBucketName,
                                    statusMessage: 'Successfully Processed',
                                    uploadFileName: srcKey,
                                    status: 'Success',
                                    outFiles: separateFileNameList,
                                    recordsProcessed: TOTAL_NO_OF_RECORDS_TO_BE_PROCESSED,
                                    filesCreated: separateFileNameList.length,
                                    createdOn: currentTime
                                }
                                console.log ( ' Success - ItemParam ' + JSON.stringify(itemParam));
                                updateDb(itemParam);
                            }
                        }
                    });
                }
            }
            callback(null, null);
        }
    });
};