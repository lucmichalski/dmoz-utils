install.packages("devtools")
devtools::install_github("themains/rdomains", build_vignettes = TRUE)

vignette("rdomains", package = "rdomains")
