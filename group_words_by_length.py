import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    words = element.strip().split(' ')
    result_list = []
    for word in words:
        result_list.append((len(word), word))
    return result_list
    
# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner') as p:

    # create a PCollection from the file contents.
    in_pcoll = p | 'Read' >> ReadFromText('input.txt')

    # apply a ParDo to the PCollection 
    word_pcoll = in_pcoll | 'ParDo' >> beam.ParDo(ComputeWordLengthFn())

    # apply GroupByKey to the PCollection
    out_pcoll = word_pcoll | 'GroupByKey' >> beam.GroupByKey()

    # write PCollection to a file
    out_pcoll | 'Write' >> WriteToText('output.txt')
    
    

    
    


