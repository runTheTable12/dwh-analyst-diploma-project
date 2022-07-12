#!/usr/bin/Rscript

dir.create(Sys.getenv("R_LIBS_USER"), recursive = TRUE)
.libPaths(Sys.getenv("R_LIBS_USER"))

options(install.packages.compile.from.source = "always")
install.packages(c("fdplyr", "tidyr", "fitzRoy"))
library(dplyr)
library(fitzRoy)
library(tidyr)

args <- commandArgs(trailingOnly = TRUE)

year <- args[1]
round <- c("Round", args[2])
round <- paste(round, collapse = " ")

round_stats <- fetch_player_stats(year, source = "footywire")
round_stats <- round_stats[round_stats$Round == round, ]

round_results <- fetch_squiggle_data(query = "games", year = year)
round_results <- round_results[round_results$round == round, year = year]

round_tips <- fetch_squiggle_data(query = "tips", year = year)
round_tips <- round_tips[round_tips$round == round, ]

write.csv(round_results, "/opt/airflow/round_results.csv", row.names = FALSE)
write.csv(round_tips, "/opt/airflow/round_tips.csv", row.names = FALSE)
write.csv(round_stats, "/opt/airflow/round_stats.csv", row.names = FALSE)
