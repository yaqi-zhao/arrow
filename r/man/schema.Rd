% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/schema.R
\name{schema}
\alias{schema}
\title{Schemas}
\usage{
schema(...)
}
\arguments{
\item{...}{\link[=field]{fields}, field name/\link[=data-type]{data type} pairs (or a list of), or object from which to extract
a schema}
}
\description{
Create a schema or extract one from an object.
}
\examples{
# Create schema using pairs of field names and data types
schema(a = int32(), b = float64())

# Create a schema using a list of pairs of field names and data types
schema(list(a = int8(), b = string()))

# Create schema using fields
schema(
  field("b", double()),
  field("c", bool(), nullable = FALSE),
  field("d", string())
)

# Extract schemas from objects
df <- data.frame(col1 = 2:4, col2 = c(0.1, 0.3, 0.5))
tab1 <- arrow_table(df)
schema(tab1)
tab2 <- arrow_table(df, schema = schema(col1 = int8(), col2 = float32()))
schema(tab2)
}
\seealso{
\link{Schema} for detailed documentation of the Schema R6 object
}
