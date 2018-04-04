const app = require("./src/app"),
  parseArgs = require("minimist");

const args = parseArgs(process.argv.slice(2));

const command = args["_"][0];

function printHelp() {
  const helpMessage = `
    Usage: node index.js COMMAND --year YEAR --month MONTH

    where COMMAND is either download-files, index-data or clean-up
    and YEAR is year in YYYY format 
    and MONTH is month number, ie for January pass 1, for October pass 10 etc
  `;

  console.log(helpMessage);
}

switch (command) {
  case "download-files":
    console.log("downloading files: ", args.year, args.month);
    app.downloadFiles({
      year: args.year,
      month: args.month
    });
    break;
  case "index-data":
    console.log("indexing data: ", args.year, args.month);
    app.indexData({ year: args.year, month: args.month });
    break;
  case "clean-up":
    console.log("removing data: ", args.year, args.month);
    break;
  default:
    printHelp();
}
