import functions from "@google-cloud/functions-framework";
import dotenv from "dotenv";
import fs from "fs";

if (fs.existsSync(".env")) {
  dotenv.config();
}

import { extract } from "./etl/extract.js";
import { transform } from "./etl/transform.js";
import { load } from "./etl/load.js";
import nodemailer from "nodemailer";
import { sendMissingDataEmail } from "./email/email.js";
import { sendMonthlyReportEmail } from "./email/extrasReport.js";

// --- Main HTTP Function ---
functions.http("runEtl", async (req, res) => {
  if (req.method !== "POST") {
    return res.status(405).send("Method Not Allowed");
  }
  try {
    console.log("💥💥💥💥💥💥💥");
    console.log("🚀 Starting ETL process...");
    const rawTables = await extract();
    console.log("✅ Extraction complete.");

    const transformedData = transform(rawTables);
    console.log("✅ Transformation complete.");
    for (const key in transformedData) {
      if (transformedData[key] && key !== "missing_data_tracker") {
        console.log(`Transformed ${key} count: ${transformedData[key].length}`);
      }
    }

    // Send missing data email if there are any missing data (only on Wednesdays)
    const now = new Date();
    const bogotaTime = new Date(
      now.toLocaleString("en-US", { timeZone: "America/Bogota" }),
    );
    const dayOfWeek = bogotaTime.getDay(); // 0 = Sunday, 3 = Wednesday

    if (
      dayOfWeek === 3 &&
      transformedData.missing_data_tracker &&
      transformedData.missing_data_tracker.size > 0
    ) {
      try {
        const transporter = nodemailer.createTransport({
          host: process.env.EMAIL_HOST,
          port: Number(process.env.EMAIL_PORT),
          auth: {
            user: process.env.EMAIL_USER,
            pass: process.env.EMAIL_PASSWORD,
          },
        });

        await sendMissingDataEmail(
          transformedData.missing_data_tracker,
          transporter,
        );
      } catch (emailError) {
        console.error(
          "⚠️ Failed to send missing data email:",
          emailError.message,
        );
        // Don't fail the entire ETL if email fails
      }
    }

    // Send monthly report email on day 10 or 25 of each month
    const dayOfMonth = bogotaTime.getDate();
    console.log("🚀 ~ dayOfMonth.......:", dayOfMonth);

    if (dayOfMonth === 10 || dayOfMonth === 24) {
      console.log("helloooooo");
      try {
        const transporter = nodemailer.createTransport({
          host: process.env.EMAIL_HOST,
          port: Number(process.env.EMAIL_PORT),
          auth: {
            user: process.env.EMAIL_USER,
            pass: process.env.EMAIL_PASSWORD,
          },
        });

        await sendMonthlyReportEmail(transformedData, rawTables, transporter);
        console.log("✅ Monthly report email sent successfully");
      } catch (emailError) {
        console.error(
          "⚠️ Failed to send monthly report email:",
          emailError.message,
        );
        // Don't fail the entire ETL if email fails
      }
    }

    // await load(transformedData);
    res.send("✅ ETL complete!");
  } catch (error) {
    console.error("❌ ETL failed:", error.message);
    if (error.stack) console.error(error.stack);
    if (error.errors) {
      error.errors.forEach((err) =>
        console.error(
          `BQ Error: ${err.message}, Reason: ${err.reason}, Location: ${err.location}`,
        ),
      );
    }
    res.status(500).send(`ETL failed: ${error.message}`);
    //
  }
});
