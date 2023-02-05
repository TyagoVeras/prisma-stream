const v4 = require('uuid').v4;
const stream = require('node:stream')
const { PrismaClient: PrismaClientV2 } = require('../prisma/generated/client')
const { PrismaClient } = require('../prisma/generated/client')

const prismaV2 = new PrismaClientV2()
const prismaCLient = new PrismaClient()

// função generator que busca os dados de forma paginada no banco de dados
// e retorna um yield para cada linha

async function* selectLinesOnStream(){
  let skip = 0;
  let take = 100;

  while(true){
    const lines = await prismaCLient.associados.findMany({
      skip,
      take,
      orderBy: {
        id: 'asc'
      }
    })

    if(lines.length === 0){
      break;
    }

    for(const line of lines){
      yield line;
    }

    skip += take;
  }
}

async function insertLineOnStream(chunk){
  await prismaV2.associados.create({
    data: chunk
  })
}

// cria um stream de leitura a partir de um generator
const readableStream = stream.Readable.from(selectLinesOnStream());

// cria um stream de transformação
const transformStream = new stream.Transform({
  objectMode: true,
  transform: (chunk, encoding, callback) => {
    chunk = {
      ...chunk,
      id: v4()
    }
    callback(null, chunk);
  }
});

// cria um stream de escrita
const writeStream = new stream.Writable({
  objectMode: true,
  write: async (chunk, encoding, callback) => {
    await insertLineOnStream(chunk);
    callback();
  }
});


// liga os streams
readableStream.pipe(transformStream).pipe(writeStream);