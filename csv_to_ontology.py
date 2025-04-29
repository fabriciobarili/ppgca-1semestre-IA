import csv
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD, OWL
from datetime import datetime


class CSVOntologyPopulator:
    def __init__(self):
        self.graph = Graph()
        self.SW = Namespace("http://www.example.org/software-review#")
        self._initialize_ontology()
        self.reviewer_cache = {}  # Cache to store reviewer URIs by their attributes

    def _initialize_ontology(self):
        """Initialize the ontology structure"""
        self.graph.bind("sw", self.SW)

        # Define classes
        classes = [self.SW.Software, self.SW.Review, self.SW.Reviewer]
        for cls in classes:
            self.graph.add((cls, RDF.type, OWL.Class))

        # Define object properties
        obj_properties = [
            (self.SW.hasReview, self.SW.Software, self.SW.Review),
            (self.SW.madeBy, self.SW.Review, self.SW.Reviewer)
        ]
        for prop, domain, range_ in obj_properties:
            self.graph.add((prop, RDF.type, OWL.ObjectProperty))
            self.graph.add((prop, RDFS.domain, domain))
            self.graph.add((prop, RDFS.range, range_))

        # Define datatype properties
        dt_properties = [
            (self.SW.software_id, self.SW.Software, XSD.string),
            (self.SW.name, self.SW.Software, XSD.string),
            (self.SW.pagina, self.SW.Software, XSD.string),
            (self.SW.fonte, self.SW.Review, XSD.string),
            (self.SW.recomendacao, self.SW.Review, XSD.string),
            (self.SW.data_avaliacao, self.SW.Review, XSD.date),
            (self.SW.comentario, self.SW.Review, XSD.string),
            (self.SW.vantagem, self.SW.Review, XSD.string),
            (self.SW.desvantagem, self.SW.Review, XSD.string),
            (self.SW.sft_anterior, self.SW.Review, XSD.string),
            (self.SW.motivo_mudanca, self.SW.Review, XSD.string),
            (self.SW.setor, self.SW.Reviewer, XSD.string),
            (self.SW.porte, self.SW.Reviewer, XSD.string),
            (self.SW.frequencia, self.SW.Reviewer, XSD.string),
            (self.SW.frequencia_complementar, self.SW.Reviewer, XSD.string)
        ]
        for prop, domain, dtype in dt_properties:
            self.graph.add((prop, RDF.type, OWL.DatatypeProperty))
            self.graph.add((prop, RDFS.domain, domain))
            self.graph.add((prop, RDFS.range, dtype))

    def _get_or_create_reviewer(self, setor, porte, frequencia, frequencia_complementar):
        """Get existing reviewer URI or create new one"""
        # Create a unique key for the reviewer
        reviewer_key = (setor, porte, frequencia, frequencia_complementar)

        if reviewer_key in self.reviewer_cache:
            return self.reviewer_cache[reviewer_key]

        # Create new reviewer
        reviewer_id = f"reviewer_{len(self.reviewer_cache) + 1}"
        reviewer_uri = self.SW[reviewer_id]

        self.graph.add((reviewer_uri, RDF.type, self.SW.Reviewer))
        self.graph.add((reviewer_uri, self.SW.setor, Literal(setor)))
        self.graph.add((reviewer_uri, self.SW.porte, Literal(porte)))
        self.graph.add((reviewer_uri, self.SW.frequencia, Literal(frequencia)))
        self.graph.add((reviewer_uri, self.SW.frequencia_complementar, Literal(frequencia_complementar)))

        self.reviewer_cache[reviewer_key] = reviewer_uri
        return reviewer_uri

    def _get_or_create_software(self, software_id, name, pagina=None):
        """Get existing software URI or create new one"""
        software_uri = self.SW[f"software_{software_id}"]

        # Check if software exists
        if (software_uri, RDF.type, self.SW.Software) not in self.graph:
            self.graph.add((software_uri, RDF.type, self.SW.Software))
            self.graph.add((software_uri, self.SW.software_id, Literal(software_id)))
            self.graph.add((software_uri, self.SW.name, Literal(name)))

            if pagina:
                self.graph.add((software_uri, self.SW.pagina, Literal(pagina)))

        return software_uri

    def process_csv(self, csv_file_path):
        """Process CSV file and populate ontology"""
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)

            for row_num, row in enumerate(reader, start=1):
                try:
                    # Process Software
                    software_uri = self._get_or_create_software(
                        software_id=row['software_id'],
                        name=row.get('name', f"Software {row['software_id']}"),
                        pagina=row.get('pagina')
                    )

                    # Process Reviewer
                    reviewer_uri = self._get_or_create_reviewer(
                        setor=row['setor'],
                        porte=row['porte'],
                        frequencia=row['frequencia'],
                        frequencia_complementar=row['frequencia_complementar']
                    )

                    # Process Review
                    review_id = f"review_{row_num}"
                    review_uri = self.SW[review_id]

                    # Convert date string to date object
                    try:
                        date_obj = datetime.strptime(row['data_avaliacao'], '%Y-%m-%d').date()
                    except ValueError:
                        date_obj = datetime.now().date()

                    # Add review triples
                    self.graph.add((review_uri, RDF.type, self.SW.Review))
                    self.graph.add((review_uri, self.SW.fonte, Literal(row['fonte'])))
                    self.graph.add((review_uri, self.SW.recomendacao, Literal(row['recomendacao'])))
                    self.graph.add((review_uri, self.SW.data_avaliacao, Literal(date_obj, datatype=XSD.date)))
                    self.graph.add((review_uri, self.SW.comentario, Literal(row['comentario'])))
                    self.graph.add((review_uri, self.SW.vantagem, Literal(row['vantagem'])))
                    self.graph.add((review_uri, self.SW.desvantagem, Literal(row['desvantagem'])))

                    if row.get('sft_anterior'):
                        self.graph.add((review_uri, self.SW.sft_anterior, Literal(row['sft_anterior'])))
                    if row.get('motivo_mudanca'):
                        self.graph.add((review_uri, self.SW.motivo_mudanca, Literal(row['motivo_mudanca'])))

                    # Create relationships
                    self.graph.add((software_uri, self.SW.hasReview, review_uri))
                    self.graph.add((review_uri, self.SW.madeBy, reviewer_uri))

                except KeyError as e:
                    print(f"Missing required column in row {row_num}: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing row {row_num}: {e}")
                    continue

    def save_ontology(self, output_file):
        """Save the ontology to a file"""
        self.graph.serialize(destination=output_file, format='xml')
        print(f"Ontology saved to {output_file}")

    def print_statistics(self):
        """Print statistics about the populated ontology"""
        software_count = len(list(self.graph.subjects(RDF.type, self.SW.Software)))
        review_count = len(list(self.graph.subjects(RDF.type, self.SW.Review)))
        reviewer_count = len(list(self.graph.subjects(RDF.type, self.SW.Reviewer)))

        print(f"Ontology contains:")
        print(f"- {software_count} software instances")
        print(f"- {review_count} reviews")
        print(f"- {reviewer_count} reviewers")


# Example usage
if __name__ == "__main__":
    populator = CSVOntologyPopulator()

    # Process your CSV file
    populator.process_csv('csv/resultados.csv')

    # Save the ontology
    populator.save_ontology('csv/SURVEILLANCE_SOFTWARE_ONTOLOGY.owl')

    # Print statistics
    populator.print_statistics()
