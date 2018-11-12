import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class NormalizeCno(beam.DoFn):
  def process(self, element, class_pcoll):
    takes_record = element
    takes_cno = takes_record.get('cno')
    cno_splits = takes_cno.split(' ')
    
    found_cno_match = False
    cno_match = None
    
    for cno_split in cno_splits:
        for class_record in class_pcoll:
            class_cno = class_record.get('cno')
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
    
    class_pcoll = p | 'Read Class' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT cno, cname, credits FROM college_split1.Class'))

    # write PCollection to a log file
    class_pcoll | 'Write to File 2' >> WriteToText('class_query_results.txt')

    # Flatten the two PCollections
    normalized_pcoll = takes_pcoll | 'Normalize cno' >> beam.ParDo(NormalizeCno(), beam.pvalue.AsList(class_pcoll)) 
    
    # write PCollection to a file
    normalized_pcoll | 'Write to File 3' >> WriteToText('output_normalize_pardo.txt')
    
    qualified_takes_table_name = 'cs327e-fa2018:college_split2.Takes'
    takes_table_schema = 'sid:STRING,cno:STRING,grade:STRING'
    
    normalized_pcoll | 'Write Takes to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_takes_table_name, 
                                                      schema=takes_table_schema,  
                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
    qualified_class_table_name = 'cs327e-fa2018:college_split2.Class'
    class_table_schema = 'cno:STRING,cname:STRING,credits:INTEGER'
    
    class_pcoll | 'Write Class to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_class_table_name, 
                                                      schema=class_table_schema,  
                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
logging.getLogger().setLevel(logging.ERROR)