const AWS = require('aws-sdk');
const awsSignRequests = require('aws-sign-requests');
const fetch = require('node-fetch');

const ES = require('elasticsearch');
const AWS_class = require('http-aws-es');

const clientconfig = {
    hosts: process.env.IS_LOCAL ? process.env.ELASTIC_HOST_LOCAL : process.env.ELASTIC_HOST,
    connectionClass: AWS_class,
    awsConfig: new AWS.Config({
        region: process.env.ELASTIC_REGION,
        credentials: new AWS.EnvironmentCredentials('AWS'),
    })
}
const client = ES.Client(clientconfig)

const bucket = new AWS.S3()


// console.log(process.env);
// console.log(clientconfig);

const tweet_root = process.env.SCRAPE_URL;
const tweet_regex = /^\/?(?:\w+\/status\/)?([0-9]+)$/i;
const limit = process.env.IS_LOCAL ? (process.env.SCRAPE_LIMIT_LOCAL || 30) : (process.env.SCRAPE_LIMIT || 3600);

const doScrape = async (event, context) => {
    var tweet_path;
    var tweet_id;

    // console.log(event);

    try {
        switch (typeof event) {
            case "object":
                if (!event.path || ! tweet_regex.test(event.path)) {
                    throw new Error('Invalid Event. Object must include valid "path" varaible.');
                }
                tweet_path = event.path.replace(/^\/+|\/+$/g, '');
                break;

            case "string":
                if (!tweet_regex.test(event)) {
                    throw new Error('Invalid Event. String must be a valid tweet path.');
                }
                event = event.replace(/^\/+|\/+$/g, '');
            case "number":
                tweet_path = event;
                break;

            default:
                console.log(typeof event);
                throw new Error ('Invalid Event');
        }

        if (!tweet_path) {
            throw new Error ('Missing Tweet Path');
        }

        switch(typeof tweet_path) {
            case 'string':
                var found = tweet_path.match(tweet_regex);
                if (!found[1]) {
                    throw new Error('Malformed Tweet String');
                }
                tweet_id = found[1];
                break;

            case 'number':
                tweet_id = tweet_path;
                break;

            default:
                throw new Error('Malformed Tweet Path.');
        }

    } catch (err) {
        console.log(err);
        // context.fail();
        return { result: -1, message: e.message, event }
    }

    let credentials = clientconfig.awsConfig.credentials;

    // console.log(credentials);

    const options = awsSignRequests({
        credentials: {
            access_key: credentials.accessKeyId,
            secret_key: credentials.secretAccessKey,
            session_token: credentials.sessionToken,
        },
        url: tweet_root + tweet_path,
    })

    // console.log(options);
    // console.log(client);

    var search = {
        index: process.env.INDEX_NAME,
        type: '_doc',
        id: tweet_id,
        _source: 'timestamp',
    };
    var existing = await client.exists(search)
        .then(res => {
            return res ? client.get(search) : false;
        })
        .catch(err => {
            console.log(err);
        })
    if (existing && existing._source && existing._source.timestamp) {
        var now = Math.floor((Date.now() / 1000 | 0 ) / limit);
        var then = Math.floor(existing._source.timestamp / limit);
        // console.log(now, then);

        if (now == then) {
            // context.done();
            return { result: 0, message : 'Limit hit', event }
        }
    }

    var json = await fetch(options.url, options)
        .then(res => res.json())
        .then(json => {
            json.timestamp = Date.now() / 1000 | 0;

            // remove nulls, so they don't overwrite previous versions of the document
            for (var key of Object.keys(json.tweetData)) {
                if (json.tweetData[key] === null) {
                    // console.log(key, json.tweetData[key]);
                    delete json.tweetData[key];
                }
            }

            return json.tweetData ? json : null
        })
        .catch(err => {
            console.log(err);
            return err.message;
        })

    // this is going to need something to store/handle bad tweet IDs

    if (!json || typeof json == 'string' || !json.tweetData) {
        // context.fail();
        return { result: -1, message: json, event }
    }

    var resp = await bucket.putObject({
        Bucket: process.env.S3_SAVE_BUCKET,
        Key: json.tweetData.tweetId + '.json',
        Body: JSON.stringify(json),
        ContentType: 'application/json',
        ACL: 'public-read',
    }).promise()
    .catch(e => {
        console.log('S3 Upload Error', e) ;
    })
    .then(up => {
        delete json.tweetData.screenshot

        if (!up) return;

        return client.update({
            index: process.env.INDEX_NAME,
            type: '_doc',
            id: json.tweetData.tweetId,
            body: {
                doc: json,
                doc_as_upsert: true
            }
        })
    })
    .then (resp => {
        return resp;
    })

    // context.succeed();
    return { result: 1, message: 'successful test', event, result: resp };
}

const mapScrape = (record) => {
    if (!/^\d+$/.test(record.body)) {
        record.body = JSON.parse(record.body);
        return doScrape(record.body)
    }
    console.log('message event', record.body);
}


module.exports.scrape = async (event, context) => {
    // console.log('successful invokation');
    console.log('event', typeof event, event);

    // await client.indices.delete({
    //     index: '*'
    // }); return;

    var result;

    if (event.Records) {
        result = [];
        for (var i in event.Records) {
            try {
                var record = event.Records[i];

                record.body = /^\d+$/.test(record.body) ? record.body : JSON.parse(record.body);
                var res = await doScrape(record.body, context);
                // console.log('res', res);

                result.push(res);
            } catch (err) {
                //
            }
        }
        // console.log('result', result);

        if (!result.length) {
            context.done();
            return;
        }

        const hasFail = result.filter(res => res.result == -1);
        if (hasFail.length > 0) {
            context.fail();
            return result;
        }

        const hasDone = result.filter(res => res.result == 0);
        if (hasDone.length > 0) {
            context.done();
            return result;
        }

        context.succeed();
        return result;

    } else {
        result = await doScrape(event, context);
        switch (result.result) {
            case 1:
                context.succeed();
                break;
            case 0:
                context.done();
                break;
            default:
                context.fail();
        }
        return result;
    }
};
