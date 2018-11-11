import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class NormalizeCno(beam.DoFn):
  def process(self, element, cno_pcoll):
    takes_record = element
    takes_cno = takes_record.get('cno')
    cno_splits = takes_cno.split(' ')
    
    found_cno_match = False
    cno_match = None
    
    for cno_split in cno_splits:
        for cno_record in cno_pcoll:
            class_cno = cno_record.get('cno')
            if (cno_split == class_cno):
                found_cno_match = True
                cno_match = cno_split
                break
        if found_cno_match == True:
            break

    if (takes_cno != cno_match):
        takes_record['cno'] = cno_match

    return [takes_record]

# Project ID is needed for bigquery data source, even with local execution.
options = {
    'project': 'cs327e-fa2018'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    takes_pcoll = p | 'Read Takes' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT sid, cno, grade FROM college_split1.Takes'))

    # write PCollection to a log file
    takes_pcoll | 'Write to File 1' >> WriteToText('takes_query_results.txt')
    
    cno_pcoll = p | 'Read Class' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT cno FROM college_split1.Class'))

    # write PCollection to a log file
    cno_pcoll | 'Write to File 2' >> WriteToText('cno_query_results.txt')

    # Flatten the two PCollections
    normalized_pcoll = takes_pcoll | 'Normalize cno' >> beam.ParDo(NormalizeCno(), beam.pvalue.AsList(cno_pcoll)) 
    
    # write PCollection to a file
    normalized_pcoll | 'Write to File 3' >> WriteToText('output_normalize_pardo.txt')
    
    qualified_table_name = 'cs327e-fa2018:college_split2.Takes'
    table_schema = 'sid:STRING,cno:STRING,grade:STRING'
    
    normalized_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                      schema=table_schema,  
                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)