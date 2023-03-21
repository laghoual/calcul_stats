import org.apache.spark.sql.DataFrame

package object comparefichiers {
  // val logger = Logger.getLogger(this.getClass.getName)

  def afficherDifferencesParColonnePrintln(leftDf: DataFrame, rightDf: DataFrame, colIdentifiant: String) {

    val columns = leftDf.columns

    val selectiveDifferences =
      columns.map(col => leftDf.selectExpr(colIdentifiant, s"$col").except(rightDf.selectExpr(colIdentifiant, s"$col")))

    println("Colonnes avec différences :")
    selectiveDifferences.foreach(diff => {
      if (diff.count > 0) {
        diff.columns.diff(List(colIdentifiant)).foreach(println)
        diff.show(false)
      }
    })

  }

  def calculerDifferencesDfPrintln(
      leftDf: DataFrame,
      rightDf: DataFrame,
      differencesParColonne: Boolean,
      colIdentifiant: String,
      dropCols: List[String] = List()
  ): Unit = {

    val leftDfCols  = leftDf.columns
    val rightDfCols = rightDf.columns

    var nbDifferencesLeftRight = -1
    var nbDifferencesRightLeft = -1

    if (leftDfCols.length != rightDfCols.length) {
      println("Les dataframes n'ont pas le même nombre de colonnes")

      println("Le premier a " + leftDfCols.length + " colonnes")
      val leftMinusRightCols = leftDfCols.diff(rightDfCols).mkString(", ")
      println("Gauche - Droite = " + leftMinusRightCols)

      println("Le deuxième a " + rightDfCols.length + " colonnes")
      val rightMinusLeftCols = rightDfCols.diff(leftDfCols).mkString(", ")
      println("Droite - Gauche = " + rightMinusLeftCols)
    } else {

      // Suppression des colonnes exclues du calcul de la différence
      val leftDfShort  = leftDf.drop(dropCols: _*)
      val rightDfShort = rightDf.drop(dropCols: _*)

      // Nombre de lignes
      println("Les dataframes ont le même nombre de colonnes")
      println("Le premier dataframe a " + leftDfShort.count + " lignes.")
      println("Le deuxième dataframe a " + rightDfShort.count + " lignes.")

      // Différence gauche - droite
      println(s"Différences Gauche - Droite")

      val leftRightExcept = leftDfShort.except(rightDfShort.selectExpr(leftDfShort.columns: _*))

      // Affichage des différences globales
      nbDifferencesLeftRight = leftRightExcept.count().toInt

      if (nbDifferencesLeftRight > 0) {
        leftRightExcept.show(false)
      }

      // Affichage des différences par colonnes
      println(s"Gauche - Droite = $nbDifferencesLeftRight valeurs différentes")
      if (differencesParColonne && nbDifferencesLeftRight > 0) {
        afficherDifferencesParColonnePrintln(leftDfShort, rightDfShort, colIdentifiant)
      }

      println(s"Différences Droite - Gauche")

      val rightLeftExcept = rightDfShort.selectExpr(leftDfShort.columns: _*).except(leftDfShort)

      // Affichage des différences globales
      nbDifferencesRightLeft = rightLeftExcept.count().toInt

      if (nbDifferencesRightLeft > 0) {
        rightLeftExcept.show(false)
      }

      // Affichage des différences par colonnes
      println(s"Droite - Gauche = $nbDifferencesRightLeft valeurs différentes\n")
      if (differencesParColonne && nbDifferencesRightLeft > 0) {
        afficherDifferencesParColonnePrintln(rightDfShort, leftDfShort, colIdentifiant)
      }
    }

  }
}
