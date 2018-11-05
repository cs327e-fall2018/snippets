import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    words = element.strip().split(' ')
    result_list = []
    for word in words:
        result = {'word' : word, 'length' : len(word)}
        result_list.append(result)
    return result_list

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner') as p:

    # create a PCollection from the file contents.
    in_pcoll = p | 'Read from File' >> ReadFromText('input.txt')

    # apply a ParDo to the PCollection 
    out_pcoll = in_pcoll | beam.ParDo(ComputeWordLengthFn())
    
    # write PCollection to a file
    out_pcoll | 'Write to File' >> WriteToText('output.txt')
    
    # write PCollection to a BQ table
    qualified_table_name = 'cs327e-fa2018:beam.Words'
    table_schema = 'word:STRING,length:INTEGER'
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
