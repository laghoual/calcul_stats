package models.bronze.tip

import datawrap.core.Variables
import org.apache.spark.sql.DataFrame
import org.scalatest.freespec.AnyFreeSpec

class TaxationPrepTest extends AnyFreeSpec {

  "[DTK-1058] Supprimer toutes les lignes du fichier Taxation dont l’un des 4 numéros de téléphone fait 8 chiffres et commence par 1" - {

    val nombreDeLignes = 100

    val colonneesFiltrees: Seq[String] = List("CalledNumber", "CallingNumber", "InitialDialledNumber", "ChargedNumber")

    /**   - [0-9]{2,7} : numéro de longueur 2 à 7
      *   - [0-9]{9,10} : numéro de longueur 9 à 10
      *   - [02-9][0-9]{7} : numéro de longueur 8 ne commençant pas par 1
      */
    val expressionNumeroValide: String   = "#{regexify '([0-9]{2,7}|[0-9]{9,10}|[02-9][0-9]{7})'}"
    val expressionNumeroInvalide: String = "#{regexify '1[0-9]{7}'}"
    val expressionNumeroNull: String     = "#{null}"

    /** Par défaut, on génère des numéros de téléphone aléatoire qui ne doivent pas être filtrés */
    val defaultGenerator =
      TaxationSource.randomOutputGenerator.configureColumns(colonneesFiltrees, expressionNumeroValide)

    "QUAND la source ne contient pas de numéro de téléphone de longueur 8 commençant par 1" - {
      "ALORS aucune ligne ne doit être filtrée" in {
        val testInputDf: DataFrame = defaultGenerator.generate(nombreDeLignes)

        /** Remplace l'input TaxationSource par le dataframe aléatoire de test qu'on vient de générer */
        TaxationPrep.mockInput(TaxationSource, testInputDf)

        /** Calcule le modèle TaxationPrep et récupère le résultat */
        val actualDf: DataFrame = TaxationPrep.run(variables = new Variables())

        /** On vérifie qu'aucune ligne n'est filtrée dans le résultat */
        assert(actualDf.count() === nombreDeLignes)
      }
    }

    def templateLesNumerosInvalidesSontFiltres(fieldName: String): Unit = {
      /* Les lignes avec des numéros invalides doivent être filtrées */
      val inputDf =
        defaultGenerator.configureColumn(fieldName, expressionNumeroInvalide).generate(nombreDeLignes)
      TaxationPrep.mockInput(TaxationSource, inputDf)
      val actualDf = TaxationPrep.run(variables = new Variables())
      /* Toutes les lignes sont filtrées */
      assert(actualDf.count() === 0)
    }

    def templateLesNumerosNullsNeSontPasFiltres(fieldName: String): Unit = {
      /* Les lignes avec des numéros invalides doivent être filtrées */
      val inputDf =
        defaultGenerator.configureColumn(fieldName, expressionNumeroNull).generate(nombreDeLignes)
      TaxationPrep.mockInput(TaxationSource, inputDf)
      val actualDf = TaxationPrep.run(variables = new Variables())
      /* Toutes les lignes sont filtrées */
      assert(actualDf.count() === nombreDeLignes)
    }

    "QUAND le champ CalledNumber contient un numéro de téléphone de longueur 8 commençant par 1" - {
      "ALORS toutes les lignes sont filtrées" in {
        templateLesNumerosInvalidesSontFiltres("CalledNumber")
      }
    }

    "QUAND le champ CallingNumber contient un numéro de téléphone de longueur 8 commençant par 1" - {
      "ALORS toutes les lignes sont filtrées" in {
        templateLesNumerosInvalidesSontFiltres("CallingNumber")
      }
    }

    "QUAND le champ InitialDialledNumber contient un numéro de téléphone de longueur 8 commençant par 1" - {
      "ALORS toutes les lignes sont filtrées" in {
        templateLesNumerosInvalidesSontFiltres("InitialDialledNumber")
      }
    }

    "QUAND le champ ChargedNumber contient un numéro de téléphone de longueur 8 commençant par 1" - {
      "ALORS toutes les lignes sont filtrées" in {
        templateLesNumerosInvalidesSontFiltres("ChargedNumber")
      }
    }

    "QUAND le champ CalledNumber contient un numéro de téléphone NULL" - {
      "ALORS aucune ligne n'est filtrée" in {
        templateLesNumerosNullsNeSontPasFiltres("CalledNumber")
      }
    }

    "QUAND le champ CallingNumber contient un numéro de téléphone NULL" - {
      "ALORS aucune ligne n'est filtrée" in {
        templateLesNumerosNullsNeSontPasFiltres("CallingNumber")
      }
    }

    "QUAND le champ InitialDialledNumber contient un numéro de téléphone NULL" - {
      "ALORS aucune ligne n'est filtrée" in {
        templateLesNumerosNullsNeSontPasFiltres("InitialDialledNumber")
      }
    }

    "QUAND le champ ChargedNumber contient un numéro de téléphone NULL" - {
      "ALORS aucune ligne n'est filtrée" in {
        templateLesNumerosNullsNeSontPasFiltres("ChargedNumber")
      }
    }
  }
}
