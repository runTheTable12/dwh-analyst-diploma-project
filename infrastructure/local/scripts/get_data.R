#!/usr/bin/Rscript

dir.create(Sys.getenv("R_LIBS_USER"), recursive = TRUE)
.libPaths(Sys.getenv("R_LIBS_USER"))

options(install.packages.compile.from.source = "always")
install.packages(c("fdplyr", "tidyr", "fitzRoy"))
library(dplyr)
library(fitzRoy)
library(tidyr)

args <- commandArgs(trailingOnly = TRUE)

# test if there is at least one argument: if not, return an error
if (length(args) == 0) {
  args <- c(2017, 2022)
  print("No arguments have been specified. Default arguments will be used.")
}

year_range <- args[1] : args[2]

player_stats <- fetch_player_stats(year_range, source = "footywire")

total_results <- data.frame()

for (i in year_range) {
  results <- fetch_squiggle_data(query = "games", year = i)
  if (i == args[2]) {
    results <- results %>% drop_na(winner)
    max_round <- max(results["round"])
  }
  total_results <- rbind(total_results, results)
}

total_tips <- data.frame()
for (i in year_range) {
  tips <- fetch_squiggle_data(query = "tips", year = i)
  if (i == args[2]) {
    tips <- tips[tips$round <= max_round + 1, ]
  }
  total_tips <- rbind(total_tips, tips)
}

write.csv(total_results, "/opt/airflow/total_results.csv", row.names = FALSE)
write.csv(total_tips, "/opt/airflow/total_tips.csv", row.names = FALSE)
write.csv(player_stats, "/opt/airflow/player_stats.csv", row.names = FALSE)