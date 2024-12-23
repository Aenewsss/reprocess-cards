import { MongoClient, ObjectId } from "mongodb";
import { Connection } from 'rabbitmq-client'
import { cards } from './cards.js'
import { environments } from "./environments.js";
import { EventEmitter } from "events"

const rabbit = new Connection(environments.RABBIT_QUEUE)
rabbit.on('error', (err) => {
    console.log('RabbitMQ connection error', err)
})
rabbit.on('connection', () => {
    console.log('Connection successfully (re)established');
});

const url = environments.MONGO_URL
const mongoClient = new MongoClient(url)
const dbName = environments.DB_NAME;

const NUM_THREADS = 100;

const eventStream = new EventEmitter();

// Escutar eventos de progresso
eventStream.on('progress', (message) => {
    console.log('📥 Progresso:', message);
});

// Dividir o array para múltiplos workers
function chunkArray(array, size) {
    const result = [];
    for (let i = 0; i < array.length; i += size) {
        result.push(array.slice(i, i + size));
    }
    return result;
}

async function sendMessageToQueue(data, queue, action) {
    let pub
    try {
        pub = rabbit.createPublisher({
            confirm: true,
            maxAttempts: 5, // Retry up to 5 times if sending fails
        });

        await pub.send(queue, {
            action,
            ...data, // Add any other message content
        });
        console.log(`Message sent to ${queue} queue`);
    } catch (err) {
        console.error('Failed to send message to queue:', err);
    } finally {
        if (pub) await pub.close(); // Ensure the publisher closes after each operation
    }
}

function checkIfIsAutomaticApproval(oldFraud, newFraud) {
    return (oldFraud.currentState == 'new' || oldFraud.currentState == 'quarantine') && newFraud.currentState == 'under_discussion'
}

async function processCard(card, ticketFraudCol, ticketFraudHistoryCol) {
    try {
        console.log('🔄 Processando card:', card);
        const ticket = await ticketFraudCol.findOne({ "card.id": card.toString() });
        if (!ticket) {
            console.warn(`⚠️ Ticket não encontrado para o card: ${card}`);
            return;
        }

        const oldFraud = await ticketFraudCol.findOne(new ObjectId(ticket._id));
        const updatedFraud = (await ticketFraudCol.findOneAndUpdate({ _id: new ObjectId(ticket._id) }, {
            $set: {
                currentState: ticket.currentState,
                isActive: ticket.currentState == 'resolved' ? false : ticket.isActive,
            }
        },
            { returnDocument: 'after' } // Para retornar o documento atualizado
        ))

        console.log('updatedFraud.card:', updatedFraud.card)

        const { responsible } = (await ticketFraudHistoryCol.find({ ticketId: ticket._id },
            {
                sort: {
                    date: -1
                },
            }
        ).toArray())[0]

        console.log('responsible:', responsible)

        await sendMessageToQueue({
            ...updatedFraud,
            _id: new ObjectId(ticket._id),
            isAutomaticApproval: checkIfIsAutomaticApproval(oldFraud, updatedFraud),
            approvedBy: responsible == 'ticketProcessor' ? new ObjectId('67196460d327c12bfe9233a4') : new ObjectId(responsible),
            statusChanged: false,
        }, environments.QUEUE_NAME, environments.QUEUE_ACTION);

        eventStream.emit('progress', `✅ Card ${card} processado com sucesso`);
    } catch (error) {
        console.error(`❌ Erro ao processar card ${card}:`, error.message);
    }
}

async function main() {
    console.log('🔗 Conectando ao servidor...');

    await mongoClient.connect();
    console.log('✅ Conectado ao MongoDB');

    try {
        const db = mongoClient.db(dbName);
        const ticketFraudCol = db.collection('ticketFraud');
        const ticketFraudHistoryCol = db.collection('fraudHistory');

        // Dividir os cards em lotes para os workers
        const cardChunks = chunkArray(cards, NUM_THREADS);

        for (const chunk of cardChunks) {
            console.log(`🛠️ Processando lote de ${chunk.length} cards...`);
            await Promise.all(chunk.map(card => processCard(card, ticketFraudCol, ticketFraudHistoryCol)));
        }

        console.log('🎯 Todos os cards foram processados com sucesso!');
    } catch (error) {
        console.error('❌ Erro no processamento:', error.message);
    } finally {
        await mongoClient.close();
        await rabbit.close();
        console.log('🔌 Conexões encerradas');
    }
}

main()
    .then(console.log)
    .catch(console.error)
    .finally(() => {
        mongoClient.close()
        rabbit.close()
    });