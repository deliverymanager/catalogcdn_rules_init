"use strict";
const AWS = require("aws-sdk");
AWS.config.update({
  region: 'eu-west-1'
});
const _ = require("lodash");
const Promise = require("bluebird");
const attr = require('dynamodb-data-types').AttributeValue;
const prune_null = require('@deliverymanager/util').prune_null;
const recursive_query_scalable = Promise.promisify(require('@deliverymanager/util').recursive_query_scalable);
const sqs = new AWS.SQS();
const {
  WebClient
} = require('@slack/client');
const web = new WebClient(process.env.SLACK_API_TOKEN);

exports.handler = async (event) => {

  class CustomError extends Error {
    constructor(...args) {
      super(...args);
      Error.captureStackTrace(this, CustomError);
      this.name = 'CustomError';
    }
  }

  try {

    //I need to be able to handle from stream of storeAccounts
    //From manual trigger with event and store_id
    const stores = [];
    console.log("event", JSON.stringify(event));

    if (!event) {
      throw new CustomError("invalid_params");
    }

    if (!_.isEmpty(event.Records)) {
      console.log("this was triggered by storeAccounts stream");
      await Promise.mapSeries(event.Records, async record => {
        let rec;
        if (record.eventName === "INSERT") {
          rec = prune_null(attr.unwrap(record.dynamodb.NewImage));
          stores.push({
            store_id: rec.store_id
          });
        } else if (record.eventName === "MODIFY") {
          rec = prune_null(attr.unwrap(record.dynamodb.NewImage));
          const rec_old = prune_null(attr.unwrap(record.dynamodb.OldImage));
          if (!_.isEqual(rec.schedule, rec_old.schedule) || !_.isEqual(rec.pickupSchedule, rec_old.pickupSchedule)) {
            stores.push({
              store_id: rec.store_id
            });
          }
        } else {
          rec = prune_null(attr.unwrap(record.dynamodb.OldImage));
          stores.push({
            store_id: rec.store_id
          });
        }


      });

    } else if (event.store_id) {

      stores.push({
        store_id: event.store_id
      });

    } else {
      throw new CustomError("invalid_params");
    }


    const putInSQS = async (messages) => {
      return await sqs.sendMessage({
        QueueUrl: 'https://sqs.eu-west-1.amazonaws.com/787324535455/catalogcdn-schedule-actions',
        MessageBody: JSON.stringify(messages)
      }).promise();
    };

    const get_all_categories = async (store_id) => {
      const params = {
        TableName: "categories",
        KeyConditionExpression: '#store_id = :store_id',
        ProjectionExpression: '#store_id, #category_id',
        ExpressionAttributeNames: {
          '#store_id': 'store_id',
          '#category_id': 'category_id'
        },
        ExpressionAttributeValues: {
          ':store_id': store_id
        }
      };

      const allCategories = await new Promise((resolve, reject) => {
        recursive_query_scalable(params, 10000, 200, 2, (err, allData) => {
          if (err) {
            console.log("err readOrdersFromDB", err);
            return reject(err);
          }
          resolve(allData);
        });
      });

      return Promise.resolve(prune_null(allCategories));
    };

    const get_all_products = async (store_id) => {

      const params = {
        TableName: 'products_new',
        KeyConditionExpression: '#store_id = :store_id',
        ProjectionExpression: '#store_id, #product_id',
        ExpressionAttributeNames: {
          '#store_id': 'store_id',
          '#product_id': 'product_id'
        },
        ExpressionAttributeValues: {
          ':store_id': store_id
        }
      };

      const allData = await new Promise((resolve, reject) => {
        recursive_query_scalable(params, 10000, 200, 2, (err, allData) => {
          if (err) {
            console.log("err readOrdersFromDB", err);
            return reject(err);
          }
          resolve(allData);
        });
      });

      const items = prune_null(allData).filter(item => !item.product_id.startsWith('template') && !item.product_id.startsWith('copy_') && !item.product_id.startsWith('test'));
      return Promise.resolve(items);
    };

    const get_all_options = async (store_id) => {

      const params = {
        TableName: 'options',
        KeyConditionExpression: '#store_id = :store_id',
        ProjectionExpression: '#store_id, #product_id_option_id',
        ExpressionAttributeNames: {
          '#store_id': 'store_id',
          '#product_id_option_id': 'product_id_option_id'
        },
        ExpressionAttributeValues: {
          ':store_id': store_id
        }
      };

      const allData = await new Promise((resolve, reject) => {
        recursive_query_scalable(params, 10000, 200, 2, (err, allData) => {
          if (err) {
            console.log("err readOrdersFromDB", err);
            return reject(err);
          }
          resolve(allData);
        });
      });

      //Filtering the items that start with init_
      const items = _.filter(prune_null(allData), (item) => {
        return !_.startsWith(item.product_id_option_id, "init_") && !_.startsWith(item.product_id_option_id, "test");
      });

      console.log("options finished", items.length);

      return Promise.resolve(items);
    };

    const get_all_choices = async (store_id) => {

      const params = {
        TableName: 'choices',
        KeyConditionExpression: '#store_id = :store_id',
        ProjectionExpression: '#store_id, #option_id_choice_id',
        ExpressionAttributeNames: {
          '#store_id': 'store_id',
          '#option_id_choice_id': 'option_id_choice_id'
        },
        ExpressionAttributeValues: {
          ':store_id': store_id
        }
      };

      const allData = await new Promise((resolve, reject) => {
        recursive_query_scalable(params, 10000, 200, 2, (err, allData) => {
          if (err) {
            console.log("err readOrdersFromDB", err);
            return reject(err);
          }
          resolve(allData);
        });
      });

      const items = _.filter(prune_null(allData), (item) => {
        return !_.startsWith(item.option_id_choice_id, "is_preselected_") && !_.startsWith(item.option_id_choice_id, "test");
      });

      return Promise.resolve(items);
    };

    const get_all_template_option_products = async (store_id, template_id_) => {
      const params = {
        TableName: 'options',
        IndexName: 'store_id-template_id_option_id-index',
        KeyConditionExpression: '#store_id = :store_id AND begins_with(#template_id_option_id, :template_id_)',
        ProjectionExpression: '#product_id_option_id, #store_id',
        ExpressionAttributeNames: {
          '#store_id': 'store_id',
          '#template_id_option_id': 'template_id_option_id',
          '#product_id_option_id': 'product_id_option_id'
        },
        ExpressionAttributeValues: {
          ':store_id': store_id,
          ':template_id_': template_id_
        }
      };

      const allData = await new Promise((resolve, reject) => {
        recursive_query_scalable(params, 10000, 200, 2, (err, allData) => {
          if (err) {
            console.log("err readOrdersFromDB", err);
            return reject(err);
          }
          resolve(allData);
        });
      });
      return Promise.resolve(prune_null(allData));
    };

    const get_all_template_choice_options = async (store_id, option_template_id_) => {
      const params = {
        TableName: 'choices',
        IndexName: 'store_id-option_template_id_choice_id-index',
        KeyConditionExpression: '#store_id = :store_id AND begins_with(#option_template_id_choice_id, :option_template_id_)',
        ProjectionExpression: '#option_id_choice_id, #store_id',
        ExpressionAttributeNames: {
          '#store_id': 'store_id',
          '#option_template_id_choice_id': 'option_template_id_choice_id',
          '#option_id_choice_id': 'option_id_choice_id'
        },
        ExpressionAttributeValues: {
          ':store_id': store_id,
          ':option_template_id_': option_template_id_
        }
      };
      const allData = await new Promise((resolve, reject) => {
        recursive_query_scalable(params, 10000, 200, 2, (err, allData) => {
          if (err) {
            console.log("err readOrdersFromDB", err);
            return reject(err);
          }
          resolve(allData);
        });
      });
      return Promise.resolve(prune_null(allData));
    };

    const handleItems = (items, table) => {
      return items.map(item => {
        item.eventName = "MODIFY";
        item.table = table;
        item.ApproximateCreationDateTime = Date.now() / 1000;
        return item;
      });
    };

    const handleTemplates = async (table, items) => {

      let messages = [];
      if (!_.isEmpty(items)) {
        await Promise.map(items, async item => {
          if (table === "options" && item.product_id_option_id.startsWith('all')) {

            const template_id_ = item.product_id_option_id.match(/all_(.+$)/)[1] + '_';
            const options = await get_all_template_option_products(item.store_id, template_id_);
            if (!_.isEmpty(options)) {
              messages = _.union(messages, handleItems(options, "options"));
            }

          } else if (table === "choices" && item.option_id_choice_id.startsWith('all')) {

            const option_template_id_ = item.option_id_choice_id.match(/all_(.+$)/)[1] + '_';
            const choices = await get_all_template_choice_options(item.store_id, option_template_id_);
            if (!_.isEmpty(choices)) {
              messages = _.union(messages, handleItems(choices, "choices"));
            }

          }
        });
      }
      return Promise.resolve(messages);
    };


    if (!_.isEmpty(stores)) {

      await Promise.mapSeries(stores, async store => {
        // This is placed so that it will delete all the rules first
        await putInSQS([{
          "store_id": store.store_id,
          "eventName": "REMOVE",
          "ApproximateCreationDateTime": Date.now() / 1000
        }]);

        let messages = [];
        //Categories
        const categories = await get_all_categories(store.store_id);
        messages = _.union(messages, handleItems(categories, "categories"));
        //Products
        const products_new = await get_all_products(store.store_id);
        messages = _.union(messages, handleItems(products_new, "products_new"));
        //Options and option templates for each product
        const options = await get_all_options(store.store_id);
        const simpleOptions = _.filter(options, item => !item.product_id_option_id.startsWith('all'));
        messages = _.union(messages, handleItems(simpleOptions, "options"));
        const templateOptions = _.filter(options, item => item.product_id_option_id.startsWith('all') && !item.product_id_option_id.startsWith('all_comments')); // because the comments do not have a distinct item for each product!
        const optionTemplateMessages = await handleTemplates("options", templateOptions);
        messages = _.union(messages, handleItems(optionTemplateMessages, "options"));
        //Choices and option templates choices for each product
        const choices = await get_all_choices(store.store_id);
        const simpleChoices = _.filter(choices, item => !item.option_id_choice_id.startsWith('all'));
        messages = _.union(messages, handleItems(simpleChoices, "choices"));
        const templateChoices = _.filter(choices, item => item.option_id_choice_id.startsWith('all'));
        const choiceTemplateMessages = await handleTemplates("choices", templateChoices);
        messages = _.union(messages, handleItems(choiceTemplateMessages, "choices"));

        if (!_.isEmpty(messages)) {
          console.log("messages", messages.length);
          const parts = _.chunk(messages, 100);
          console.log("parts", parts.length);
          await Promise.map(parts, async part => {
            return await putInSQS(part);
          });

          await web.chat.postMessage({
            channel: "C6VE2A6PQ",
            text: `NEW SYSTEM: Initialized all catalog rules for store_id: ${store.store_id} (lambda: initialize_catalog_rules)`
          });

        }
        return Promise.resolve();
      });
    }


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