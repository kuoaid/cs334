#include <iostream>
#include "p0_starter.h"
namespace bustub{
    template <typename T>
    int main()
    {
        int arr1[6] = {1, 2, 3, 4, 5, 6};
        std::unique_ptr<RowMatrix<int>> mat1_ptr{new RowMatrix<int>(2, 3)};
        mat1_ptr->MatImport(&arr1[0]);
        std::cout << "hi" << std::endl;
        std::cout << mat1_ptr->GetElem(1,1) << std::endl;
    return 0;
    }
}
