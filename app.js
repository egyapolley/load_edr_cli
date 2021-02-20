const fs = require("fs");
const path = require("path");


const moment = require("moment");

const sequelize = require("./models/sql_database");
const RechargesEDR = require("./models/sql_models").RechargesEDR;
const ActivationEDR = require("./models/sql_models").ActivationEDR;
const ExpiredEDR = require("./models/sql_models").ExpiredEDR;

const inputDir = path.join(__dirname, "input_dir");
const outputDir = path.join(__dirname,"output_dir")

sequelize.sync({
    force:true
}).then(() =>{
    console.log("sequelize connected");
    fs.readdir(inputDir,(dirError, files) => {
        if (dirError) throw dirError;
        if (files.length >0){
            for (const file of files) {
                let inputFilePath = path.join(inputDir,file);
                let outputFilePath = path.join(outputDir,file);

                fs.readFile(inputFilePath,{ encoding:"utf-8"}, async (fileError, data) => {
                    if (fileError) throw fileError;
                    const dataArray = data.trim().split("\n");
                    if (file.startsWith("recharges")){
                        for (const dataString of dataArray) {
                            let tempArray = dataString.split("|");
                            let msisdn = tempArray[0];
                            let dateOfRecharge = moment(tempArray[1],"YYYYMMDDHHmmss").format("DD-MM-YYYY HH:mm:ss")
                            let voucherType =tempArray[2];
                            try {
                                await RechargesEDR.create({msisdn,dateOfRecharge,voucherType});
                            }catch (error){
                                console.log(error)
                            }



                        }

                    }else if (file.startsWith("expiry")){
                        for (const dataString of dataArray) {
                            let tempArray = dataString.split("|");
                            let msisdn = tempArray[0];
                            let dateExpired =  moment(tempArray[1],"YYYYMMDDHHmmss").format("DD-MM-YYYY HH:mm:ss");
                            let value =tempArray[2];
                            try {
                                await ExpiredEDR.create({msisdn,dateExpired,value});

                            }catch (error){
                                console.log(error)
                            }


                        }


                    }else if (file.startsWith("activation")){
                        for (const dataString of dataArray) {
                            let tempArray = dataString.split("|");
                            let msisdn = tempArray[0];
                            let dateOfActivation =  moment(tempArray[1],"YYYYMMDDHHmmss").format("DD-MM-YYYY HH:mm:ss");
                            let dateOfExpiry=moment(tempArray[1],"YYYYMMDDHHmmss").add(30,"days").format("DD-MM-YYYY HH:mm:ss");
                            try {
                                await ActivationEDR.create({msisdn,dateOfActivation,dateOfExpiry})
                            }catch (error){
                                console.log(error)
                            }

                        }

                    }

                })

                fs.rename(inputFilePath,outputFilePath, err => {
                    if (err) console.log(file +" movement failed");
                })

            }

        }

    })
}).catch(error => console.log(error))


