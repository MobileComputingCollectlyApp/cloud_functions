const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { Firestore } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
admin.initializeApp();
const express = require('express');
const cookieParser = require('cookie-parser')();
const cors = require('cors')({ origin: true });
const app = express();
const archiver = require('archiver');

const IMAGE_TYPES = ["jpg", "png", "svg"];
const AUDIO_TYPES = ["mp3"];
const VIDEO_TYPES = ["mp4"];
const DOCUMENT_TYPES = ["pdf"];

// Express middleware that validates Firebase ID Tokens passed in the Authorization HTTP header.
// The Firebase ID token needs to be passed as a Bearer token in the Authorization HTTP header like this:
// `Authorization: Bearer <Firebase ID Token>`.
// when decoded successfully, the ID Token content will be added as `req.user`.
const validateFirebaseIdToken = async (req, res, next) => {
    functions.logger.log('Check if request is authorized with Firebase ID token');

    if ((!req.headers.authorization || !req.headers.authorization.startsWith('Bearer ')) &&
        !(req.cookies && req.cookies.__session)) {
        functions.logger.error(
            'No Firebase ID token was passed as a Bearer token in the Authorization header.',
            'Make sure you authorize your request by providing the following HTTP header:',
            'Authorization: Bearer <Firebase ID Token>',
            'or by passing a "__session" cookie.'
        );
        res.status(403).send('Unauthorized');
        return;
    }

    let idToken;
    if (req.headers.authorization && req.headers.authorization.startsWith('Bearer ')) {
        functions.logger.log('Found "Authorization" header');
        // Read the ID Token from the Authorization header.
        idToken = req.headers.authorization.split('Bearer ')[1];
    } else if (req.cookies) {
        functions.logger.log('Found "__session" cookie');
        // Read the ID Token from cookie.
        idToken = req.cookies.__session;
    } else {
        // No cookie
        res.status(403).send('Unauthorized');
        return;
    }

    try {
        const decodedIdToken = await admin.auth().verifyIdToken(idToken);
        functions.logger.log('ID Token correctly decoded', decodedIdToken);
        req.user = decodedIdToken;
        next();
        return;
    } catch (error) {
        functions.logger.error('Error while verifying Firebase ID token:', error);
        res.status(403).send('Unauthorized');
        return;
    }
};

// app.use(cors);
// app.use(cookieParser);
// app.use(validateFirebaseIdToken);
app.get('/generate-dataset', async (req, res) => {
    // Read project_id and form_id from query parameters
    if (req.query?.project_id && req.query?.form_id) {
        project_id = req.query.project_id;
        form_id = req.query.form_id;
    } else {
        functions.logger.error('Parameters, project_id and form_id are not received:');
        res.status(400).send('Invalid Request');
        return;
    }

    // Initiate firestore and storage client
    const firestore = new Firestore({
        projectId: admin.instanceId().app.options.projectId,
    });
    const storage = new Storage({
        projectId: admin.instanceId().app.options.projectId,
    });

    const collection = firestore.collection(`projects/${project_id}/forms/${form_id}/data`);
    const documents = await collection.listDocuments();

    const bucket = storage.bucket(`${admin.instanceId().app.options.projectId}.appspot.com`);

    // Create output zip file
    const outputStreamBuffer = bucket.file(`public/downloads/${project_id}_${form_id}.zip`).createWriteStream({
        gzip: true,
        contentType: 'application/zip',
    });

    // Initiate the archiver
    const archive = archiver('zip', {
        gzip: true,
        zlib: { level: 9 },
    });

    archive.on('error', (err) => {
        functions.logger.error('A compression error is occured:', err);
        res.status(400).send('Compression Error');
    });

    archive.pipe(outputStreamBuffer);

    // Add all the resources related to the current form into the zip file
    for (let i = 0; i < documents.length; i++) {
        const document = firestore.doc(`projects/${project_id}/forms/${form_id}/data/${documents[i]._path.segments[5]}`);
        const data = await document.get();

        archive.append(JSON.stringify(data._fieldsProto), {
            name: `${documents[i]._path.segments[5]}.json`,
        });

        for (let key of Object.keys(data._fieldsProto)) {
            if (data._fieldsProto[key].stringValue) {
                const splits = data._fieldsProto[key].stringValue.split(".");
                const type = splits[splits.length - 1];
                let dir = "";

                if (IMAGE_TYPES.includes(type)) {
                    dir = "images";
                } else if (AUDIO_TYPES.includes(type)) {
                    dir = "audios";
                } else if (VIDEO_TYPES.includes(type)) {
                    dir = "videos";
                } else if (DOCUMENT_TYPES.includes(type)) {
                    dir = "documents";
                }

                if (dir) {
                    const content = await storage
                        .bucket(`${admin.instanceId().app.options.projectId}.appspot.com`)
                        .file(`private/${dir}/${data._fieldsProto[key].stringValue}`)
                        .download();
                    archive.append(content[0], {
                        name: `${dir}/${data._fieldsProto[key].stringValue}`,
                    });
                }
            }
        }
    }

    archive.on('finish', async () => {
        res.status(200).send('Dataset generation succeeded');
    });

    await archive.finalize();
});

// This HTTPS endpoint can only be accessed by your Firebase Users.
// Requests need to be authorized by providing an `Authorization` HTTP header
// with value `Bearer <Firebase ID Token>`.
exports.app = functions.https.onRequest(app);