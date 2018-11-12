import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
    
class MakeTuple(beam.DoFn):
  def process(self, element):
    record = element
    sid_val = record.get('sid')
    record.pop('sid')
    sid_tuple = ({'sid': sid_val}, record)
    return [sid_tuple]
  
class MakeRecord(beam.DoFn):
  def process(self, element, class_pcoll):
    key, val = element
    sid_val = key.get('sid')

    for student_records in val:
        for student_record in student_records:
            if 'lname' in student_record:
                student_record['sid'] = sid_val
            if 'cno' in student_record:
                cno_val = student_record.get('cno')
                for class_record in class_pcoll:
                    class_cno_val = class_record.get('cno')
                    if cno_val == class_cno_val:
                        cname_val = class_record.get('cname')
                        student_record['cname'] = cname_val
    return [val]

# Project ID is needed for bigquery data source, even with local execution.
options = {
    'project': 'cs327e-fa2018'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    student_pcoll = p | 'Read Student' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM college_split2.Deduped_Student'))
    takes_pcoll = p | 'Read Takes' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM college_split2.Takes'))
    class_pcoll = p | 'Read Class' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT cno, cname FROM college_split2.Class'))
    
    student_tuple_pcoll = student_pcoll | 'Create Sid Student Tuple' >> beam.ParDo(MakeTuple())
    takes_tuple_pcoll = takes_pcoll | 'Create Sid Takes Tuple' >> beam.ParDo(MakeTuple())
    
    student_tuple_pcoll | 'Write to File 1' >> WriteToText('output_sid_student_tuple.txt')
    takes_tuple_pcoll | 'Write to File 2' >> WriteToText('output_sid_takes_tuple.txt')

    # Join Student and Takes on sid key 
    joined_sid_pcoll = (student_tuple_pcoll, takes_tuple_pcoll) | 'Join Student and Takes' >> beam.CoGroupByKey()
    joined_sid_pcoll | 'Write to File 3' >> WriteToText('output_joined_sid_pcoll.txt')
    
    # Join Results with Class on cno 
    student_records_pcoll = joined_sid_pcoll | 'Add Cname to Student Record' >> beam.ParDo(MakeRecord(), 
                                                                                           beam.pvalue.AsList(class_pcoll))
    student_records_pcoll | 'Write to File 4' >> WriteToText('output_student_records_pcoll.txt')
        

logging.getLogger().setLevel(logging.ERROR)