exports = function(payload, response) {
    const decodeBase64 = (s) => {
        var e = {},
            i, b = 0,
            c, x, l = 0,
            a, r = '',
            w = String.fromCharCode,
            L = s.length
        var A = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        for (i = 0; i < 64; i++) {
            e[A.charAt(i)] = i
        }
        for (x = 0; x < L; x++) {
            c = e[s.charAt(x)];
            b = (b << 6) + c;
            l += 6
            while (l >= 8) {
                ((a = (b >>> (l -= 8)) & 0xff) || (x < (L - 2))) && (r += w(a))
            }
        }
        return r;
    }

    var fullDocument = JSON.parse(payload.body.text());

    const firehoseAccessKey = payload.headers["X-Amz-Firehose-Access-Key"]

    // Check shared secret is the same to validate Request source
    if (firehoseAccessKey == context.values.get("IOTPOC_SECRET_KEY")) {

        var collection = context.services.get("mongodb-atlas").db("dbName").collection("collectionName");
		
		/*we are creating bucket pattern manually and inserting data. 
		  we check if document already exists, if exists we append data to existing array, else we create a new document.

		  If your using the timeseries collction in mongo 5.0, you can insert data using "const status = collection.insertOne(fullDocument);"*/
		  
        fullDocument.records.forEach((record) => {
            const document = JSON.parse(decodeBase64(record.data))
			
			//date and transactionDate are of type string. We convert it to ISODate format
            document.transaction.transactionDate = new Date(document.transaction.transactionDate);
            document.date = new Date(document.date);

            const query = {
                "date": document.date,
                "tollPointId": document.tollPointId,
                "laneNumber": document.laneNumber
            };
            const projection = {
                "tollPointId": 1
            };

            const item = collection.findOne(query, projection).then(item => {
                if (item) {
                    const status = collection.updateOne({
                        "date": document.date,
                        "tollPointId": document.tollPointId,
                        "laneNumber": document.laneNumber
                    }, {
                        $addToSet: {
                            "transactions": document.transaction
                        }
                    }, {
                        upsert: true
                    });
                } else {
                    document.transactions = [document.transaction];
                    delete document.transaction;
                    const status = collection.insertOne(document);
                }
            });
        })

        response.setStatusCode(200)
        const s = JSON.stringify({
            requestId: payload.headers['X-Amz-Firehose-Request-Id'][0],
            timestamp: (new Date()).getTime()
        })
        response.addHeader(
            "Content-Type",
            "application/json"
        );
        response.setBody(s)
        return
    } else {
        response.setStatusCode(500)
        response.setBody(JSON.stringify({
            requestId: payload.headers['X-Amz-Firehose-Request-Id'][0],
            timestamp: (new Date()).getTime(),
            errorMessage: "Error authenticating"
        }))
        return
    }
};