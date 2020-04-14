"use strict";
const AWS = require("aws-sdk");
AWS.config.update({
  region: 'eu-west-1'
});
const sqs = new AWS.SQS();

exports.handler = async () => {

  class CustomError extends Error {
    constructor(...args) {
      super(...args);
      Error.captureStackTrace(this, CustomError);
      this.name = 'CustomError';
    }
  }

  try {

    //I need to be able to handle from stream of storeAccounts
    //From manual trigger with event and store_id group

    


    console.log("RequestId SUCCESS");

    return {
      success: true
    };

  } catch (err) {
    console.log("err", err);
    return {
      success: false,
      comments: "Σφάλμα",
      comment_id: err instanceof CustomError ? err.message : "classic_error"
    };
  }
};